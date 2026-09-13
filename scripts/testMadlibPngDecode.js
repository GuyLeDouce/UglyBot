'use strict';
// Synthetic fixtures only. No user artwork, production credentials or external services.
const fs=require('node:fs'),zlib=require('node:zlib');
const {Readable}=require('node:stream');
const {assert,harness,errorCode}=require('./madlibTestUtils');
const {createCanvas,loadImage}=require('@napi-rs/canvas');
const images=require('../modules/madlibImages');
const {decodeRaster,failureReason}=require('../modules/madlibImageWorker');
const run=harness('png-decode'),limit=8388608;
function crc32(b){let c=0xffffffff;for(const byte of b){c^=byte;for(let j=0;j<8;j++)c=(c>>>1)^((c&1)?0xedb88320:0);}return (c^0xffffffff)>>>0;}
function chunk(type,data){const name=Buffer.from(type),len=Buffer.alloc(4),crc=Buffer.alloc(4);len.writeUInt32BE(data.length);crc.writeUInt32BE(crc32(Buffer.concat([name,data])));return Buffer.concat([len,name,data,crc]);}
const canvas=createCanvas(64,64);canvas.getContext('2d').fillRect(5,5,30,30);
const png=canvas.toBuffer('image/png');
const tagged=(type,text)=>Buffer.concat([png.subarray(0,33),chunk(type,Buffer.from(text)),png.subarray(33)]);
const c2pa=tagged('caBX','Synthetic content-provenance fixture: <svg xmlns="http://www.w3.org/2000/svg"/>');
const decode=b=>images.normalizeImage(b,images.dimensions(b),limit);
const attachment=b=>({url:'https://cdn.discordapp.com/ephemeral-attachments/1290584204689801267/1334884237727240267/download.png?ex=123&is=456&hm=synthetic',contentType:'image/png',size:b.length});
(async()=>{
  console.log(`Decoder runtime: node=${process.versions.node} canvas=${require('@napi-rs/canvas/package.json').version}`);
  await run.test('report the previous Buffer path separately from fixed decoding',async()=>{
    assert.equal((await loadImage(png)).width,64);
    let result='accepted';try{await loadImage(c2pa);}catch(error){result=failureReason(error);}
    console.log(`Previous Buffer path with synthetic caBX/SVG metadata: ${result}`);
    // Known production-compatible dependency version must reproduce this exact regression.
    if(require('@napi-rs/canvas/package.json').version==='0.1.100')assert.equal(result,'SVG_MISDETECTED');
    assert.equal((await decode(c2pa)).width,64);
  });
  await run.test('use a locally constructed raster data URL directly, never loadImage wrapping or a user URL',async()=>{
    let received;
    class FakeImage{set src(value){received=value;this.width=64;this.height=64;this.onload();}}
    assert.equal((await decodeRaster(FakeImage,c2pa,images.dimensions(c2pa))).width,64);
    assert(received.startsWith('data:image/png;base64,'));assert.deepEqual(Buffer.from(received.split(',')[1],'base64'),c2pa);
    await assert.rejects(()=>decodeRaster(FakeImage,c2pa,{media_type:'image/svg+xml'}));
    await assert.rejects(()=>decodeRaster(FakeImage,'https://example.invalid/image.png',{media_type:'image/png'}));
  });
  await run.test('PNG metadata and trailing SVG markers do not change pixels or the input buffer',async()=>{
    const clean=await decode(png);
    const variants=[c2pa,tagged('tEXt','Comment\0<svg xmlns="http://www.w3.org/2000/svg"/>'),tagged('iTXt','Comment\0\0\0\0\0<svg xmlns="http://www.w3.org/2000/svg"/>'),Buffer.concat([png,Buffer.from('<svg xmlns')])];
    for(const b of variants){const original=Buffer.from(b),out=await decode(b);assert.deepEqual(b,original);assert.deepEqual(out.bytes,clean.bytes);assert.equal(out.media_type,'image/png');}
  });
  await run.test('an SVG-looking byte sequence inside real PNG pixel data is not treated as vector input',async()=>{
    const raw=Buffer.alloc(64*(1+64*4));
    Buffer.from('<svg xmlns').copy(raw,1);
    const idat=zlib.deflateSync(raw,{level:0});assert(idat.includes(Buffer.from('<svg')));
    const b=Buffer.concat([png.subarray(0,33),chunk('IDAT',idat),chunk('IEND',Buffer.alloc(0))]);
    const out=await decode(b);assert.equal(out.width,64);assert.equal(out.height,64);assert.equal(images.dimensions(out.bytes).media_type,'image/png');
  });
  await run.test('full attachment download path accepts provenance PNGs and preserves signed request parameters',async()=>{
    const a=attachment(c2pa);let calls=0;
    const out=await images.downloadAttachment({...a,contentType:'application/octet-stream'},limit,{fetcher:async(url,options)=>{
      calls++;assert.equal(url,a.url);assert.equal(options.redirect,'manual');
      return {status:200,headers:{get:()=>String(c2pa.length)},body:Readable.from([c2pa.subarray(0,50),c2pa.subarray(50)])};
    }});
    assert.equal(calls,1);assert.equal(out.width,64);assert.equal(out.height,64);assert(out.bytes.length<=limit);
  });
  await run.test('SVG HTML corrupt files and over-limit images still fail; a later valid PNG still works',async()=>{
    for(const b of [Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"/>'),Buffer.from('<html>not a raster image at all</html>')])assert.throws(()=>images.dimensions(b),errorCode('IMAGE'));
    await assert.rejects(()=>decode(png.subarray(0,33)),errorCode('IMAGE'));
    await assert.rejects(()=>images.normalizeImage(png,images.dimensions(png),1),errorCode('IMAGE_SIZE'));
    assert.throws(()=>images.validateAttachment({...attachment(png),size:limit+1}),errorCode('IMAGE_SIZE'));
    const b=Buffer.from(png);b.writeUInt32BE(4097,16);assert.throws(()=>images.dimensions(b),errorCode('IMAGE'));
    assert.equal((await decode(png)).width,64);
  });
  await run.test('decoder failures expose only fixed stage categories and validated version strings',()=>{
    const logs=[],warn=console.warn;console.warn=line=>logs.push(line);
    try{
      const e=images.decoderFailure({ok:false,diagnostics:{stage:'INIT',reason:'NATIVE_BINDING',canvasVersion:'0.1.100'}});
      assert.equal(e.code,'IMAGE');assert(e.message.includes('IMAGE_INIT'));assert(logs[0].includes('stage=INIT reason=NATIVE_BINDING canvas=0.1.100'));
      const other=images.decoderFailure({diagnostics:{stage:'SECRET_DATABASE_URL',reason:'SECRET_TOKEN',canvasVersion:'https://secret.invalid/?token=SECRET'}});
      assert(!JSON.stringify([logs,e.message,other.message]).includes('SECRET'));
      assert.equal(failureReason(new Error('Invalid SVG image')),'SVG_MISDETECTED');
      assert.equal(failureReason(new Error('Cannot find module /private/secret')),'NATIVE_BINDING');
    }finally{console.warn=warn;}
  });
  if(process.env.MADLIB_TEST_IMAGE_PATH){
    await run.test('owner-supplied local original decodes without re-exporting it or publishing the file',async()=>{
      const b=fs.readFileSync(process.env.MADLIB_TEST_IMAGE_PATH),before=Buffer.from(b),info=images.dimensions(b);
      assert(b.length<=limit);const out=await decode(b);assert.deepEqual(before,b);
      assert.equal(out.width,info.width);assert.equal(out.height,info.height);assert(out.bytes.length<=limit);
      console.log(`Original-file result: ${info.width}x${info.height}, input=${b.length}, normalized=${out.bytes.length} bytes`);
    });
  }
  run.done();
})().catch(error=>{console.error(error.stack);process.exitCode=1;});

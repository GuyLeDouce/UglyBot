'use strict';
const path=require('node:path');
const {Worker}=require('node:worker_threads');
const fetch=require('node-fetch');
const {check,MadlibError}=require('./madlibCore');
const MIME=new Set(['image/png','image/jpeg','image/webp']);
function dimensions(bytes) {
  check(Buffer.isBuffer(bytes)&&bytes.length>=24,'IMAGE','That file is not a supported image.');let type,width,height;
  if(bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))){
    check(bytes.toString('ascii',12,16)==='IHDR'&&bytes.length>=33,'IMAGE','Invalid PNG header.');
    width=bytes.readUInt32BE(16);height=bytes.readUInt32BE(20);type='image/png';
    for(let p=8;p+12<=bytes.length;){const len=bytes.readUInt32BE(p);check(len<=bytes.length-p-12,'IMAGE','Invalid PNG chunk.');check(bytes.toString('ascii',p+4,p+8)!=='acTL','IMAGE','Upload a still image, not an animated PNG.');p+=len+12;}
  }else if(bytes[0]===0xff&&bytes[1]===0xd8){
    type='image/jpeg';let p=2;
    while(p<bytes.length){check(bytes[p]===0xff,'IMAGE','Invalid JPEG marker.');while(bytes[p]===0xff)p++;const marker=bytes[p++];if(marker===0xd9||marker===0xda)break;if(marker===0x01||(marker>=0xd0&&marker<=0xd7))continue;
      check(p+2<=bytes.length,'IMAGE','Truncated JPEG.');const len=bytes.readUInt16BE(p);check(len>=2&&p+len<=bytes.length,'IMAGE','Invalid JPEG segment.');
      if([0xc0,0xc1,0xc2].includes(marker)){check(len>=8,'IMAGE','Invalid JPEG frame.');height=bytes.readUInt16BE(p+3);width=bytes.readUInt16BE(p+5);break;}p+=len;}
  }else if(bytes.toString('ascii',0,4)==='RIFF'&&bytes.toString('ascii',8,12)==='WEBP'){
    type='image/webp';check(bytes.readUInt32LE(4)+8===bytes.length,'IMAGE','Invalid WebP length.');const tag=bytes.toString('ascii',12,16);
    if(tag==='VP8X'){check(bytes.length>=30&&!(bytes[20]&2),'IMAGE','Use a still WebP image.');width=1+bytes.readUIntLE(24,3);height=1+bytes.readUIntLE(27,3);}
    else if(tag==='VP8 '){check(bytes.length>=30&&bytes.subarray(23,26).equals(Buffer.from([157,1,42])),'IMAGE','Invalid WebP frame.');width=bytes.readUInt16LE(26)&16383;height=bytes.readUInt16LE(28)&16383;}
    else if(tag==='VP8L'){check(bytes.length>=25&&bytes[20]===47,'IMAGE','Invalid lossless WebP frame.');const bits=bytes.readUInt32LE(21);width=(bits&16383)+1;height=((bits>>>14)&16383)+1;}
  }
  check(type&&Number.isInteger(width)&&Number.isInteger(height)&&width>=32&&height>=32&&width<=4096&&height<=4096&&width*height<=16000000,'IMAGE','Use a still PNG, JPEG or WebP, 32–4096 pixels per side and at most 16 million pixels.');return {media_type:type,width,height};
}
function validateAttachment(a,maxBytes=8388608){
  check(a&&typeof a.url==='string'&&Number.isSafeInteger(a.size)&&a.size>0&&a.size<=maxBytes,'IMAGE_SIZE',`Attach one image no larger than ${Math.floor(maxBytes/1048576)} MiB.`);
  check(MIME.has(String(a.contentType||'').split(';')[0].toLowerCase()),'IMAGE_TYPE','Upload a PNG, JPEG or WebP as a Discord attachment.');
  let u;try{u=new URL(a.url);}catch(_){throw new MadlibError('IMAGE_URL','Invalid Discord attachment URL.');}
  check(u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['cdn.discordapp.com','media.discordapp.net'].includes(u.hostname)&&/^\/attachments\/\d{17,20}\/\d{17,20}\/[^/]+$/.test(u.pathname),'IMAGE_URL','Only direct Discord attachment downloads are allowed.');return u.toString();
}
let activeDecodes=0;
async function normalizeImage(bytes,info,maxBytes){
  check(activeDecodes<2,'IMAGE_BUSY','Two images are already being checked. Try this upload again in a moment.');activeDecodes++;
  try{return await new Promise((resolve,reject)=>{
    const w=new Worker(path.join(__dirname,'madlibImageWorker.js'),{workerData:{bytes,info,maxBytes},resourceLimits:{maxOldGenerationSizeMb:128}});let done=false;
    const finish=(err,data)=>{if(done)return;done=true;clearTimeout(timer);w.terminate().catch(()=>{});if(err)reject(err);else resolve({...info,media_type:'image/png',bytes:Buffer.from(data)});};
    const timer=setTimeout(()=>finish(new MadlibError('IMAGE','Image decoding timed out. Try a smaller still image.')),5000);
    w.once('message',m=>m.ok?finish(null,m.bytes):finish(new MadlibError('IMAGE','That image could not be decoded safely. Try re-exporting it as PNG.')));
    w.once('error',()=>finish(new MadlibError('IMAGE','That image could not be decoded safely.')));
    w.once('exit',code=>{if(!done)finish(new MadlibError('IMAGE',`Image checker exited before completion (${code}).`));});
  });}finally{activeDecodes--;}
}
async function downloadAttachment(attachment,maxBytes=8388608,{fetcher=fetch,decode=normalizeImage}={}){
  const url=validateAttachment(attachment,maxBytes),abort=new AbortController();const timer=setTimeout(()=>abort.abort(),12000);let res;
  try{
    res=await fetcher(url,{signal:abort.signal,redirect:'manual',size:maxBytes,timeout:12000});check(res.status===200,'IMAGE_DOWNLOAD','Discord could not supply the attachment. Upload it again.');
    const contentLength=Number(res.headers.get('content-length')||0);check(Number.isFinite(contentLength)&&contentLength<=maxBytes,'IMAGE_SIZE','Image exceeds the upload limit.');
    const chunks=[];let total=0;for await(const chunk of res.body){total+=chunk.length;check(total<=maxBytes,'IMAGE_SIZE','Image exceeds the upload limit.');chunks.push(chunk);}
    const bytes=Buffer.concat(chunks),info=dimensions(bytes);check(info.media_type===attachment.contentType.split(';')[0].toLowerCase(),'IMAGE_TYPE','The file signature does not match the declared image type.');return await decode(bytes,info,maxBytes);
  }catch(e){if(e instanceof MadlibError)throw e;throw new MadlibError('IMAGE_DOWNLOAD','The upload could not be downloaded safely. Attach the image again.');}
  finally{clearTimeout(timer);abort.abort();res?.body?.destroy?.();}
}
module.exports={dimensions,validateAttachment,normalizeImage,downloadAttachment};

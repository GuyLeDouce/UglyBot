'use strict';
const path=require('node:path');
const {fork}=require('node:child_process');
const fetch=require('node-fetch');
const {check,MadlibError}=require('./madlibCore');
// Attachment MIME is optional. File signatures and decoding establish the real format.
const MIME=new Set(['image/png','image/x-png','image/jpeg','image/jpg','image/pjpeg','image/jfif','image/webp','image/x-webp','application/octet-stream','binary/octet-stream','application/binary','']);
const MAX_PIXELS=4096*4096;
const typeHint=a=>String(a?.contentType??a?.content_type??'').split(';')[0].trim().toLowerCase();
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
  check(type&&Number.isInteger(width)&&Number.isInteger(height)&&width>=32&&height>=32&&width<=4096&&height<=4096&&width*height<=MAX_PIXELS,'IMAGE','Use a still PNG, JPEG or WebP, 32–4096 pixels per side including 4096 × 4096.');return {media_type:type,width,height};
}
function validateAttachment(a,maxBytes=8388608){
  check(a&&typeof a.url==='string'&&Number.isSafeInteger(a.size)&&a.size>0&&a.size<=maxBytes,'IMAGE_SIZE',`Attach one image no larger than ${Math.floor(maxBytes/1048576)} MiB.`);
  check(MIME.has(typeHint(a)),'IMAGE_TYPE','Attach a still PNG, JPG/JPEG or WebP image (not a webpage, SVG, animation, or document).');
  let u;try{u=new URL(a.url);}catch(_){throw new MadlibError('IMAGE_URL','Invalid Discord attachment URL.');}
  // Slash-command uploads can use /ephemeral-attachments/ rather than /attachments/.
  // Keep the exact Discord hosts and signed query string; do not rewrite paths or follow redirects.
  check(u.protocol==='https:'&&!u.username&&!u.password&&!u.port&&['cdn.discordapp.com','media.discordapp.net'].includes(u.hostname)&&/^\/(?:ephemeral-)?attachments\/\d{17,20}\/\d{17,20}\/[^/]+$/.test(u.pathname),'IMAGE_URL','Only direct Discord attachment downloads are allowed.');return u.toString();
}
let activeDecodes=0;
async function normalizeImage(bytes,info,maxBytes){
  check(activeDecodes<2,'IMAGE_BUSY','Two images are already being checked. Try this upload again in a moment.');activeDecodes++;
  try{return await new Promise((resolve,reject)=>{
    // Native decoder crashes affect worker threads' entire process. Use a child instead.
    // This is crash containment, not a security sandbox; all input bounds still apply.
    const child=fork(path.join(__dirname,'madlibImageWorker.js'),[],{
      execArgv:['--max-old-space-size=128'],serialization:'advanced',stdio:['ignore','ignore','ignore','ipc'],
      // No application credentials or inherited NODE_OPTIONS are needed by this decoder.
      env:{PATH:process.env.PATH||'',LANG:'C.UTF-8'},
    });
    let result=null,failure=null,settled=false;
    const terminate=()=>{if(!child.killed)child.kill('SIGKILL');};
    const timer=setTimeout(()=>{failure=new MadlibError('IMAGE','Image decoding timed out. Try a smaller still image.');terminate();},5000);
    child.once('message',m=>{
      const valid=m?.ok&&Buffer.isBuffer(m.bytes)&&m.bytes.length>0&&m.bytes.length<=maxBytes&&
        ((m.width===info.width&&m.height===info.height)||(info.media_type==='image/jpeg'&&m.width===info.height&&m.height===info.width));
      if(valid)result={media_type:'image/png',width:m.width,height:m.height,bytes:m.bytes};
      else failure=new MadlibError(m?.code==='IMAGE_SIZE'?'IMAGE_SIZE':'IMAGE',m?.code==='IMAGE_SIZE'?'This image becomes too large when prepared for Discord. Upload a smaller image.':'That file could not be decoded as a still PNG, JPG/JPEG or WebP image.');
      terminate();
    });
    child.once('error',()=>{failure=new MadlibError('IMAGE','The image checker could not start. Try again in a moment.');terminate();});
    child.once('close',()=>{
      if(settled)return;settled=true;clearTimeout(timer);
      if(failure)reject(failure);else if(result)resolve(result);else reject(new MadlibError('IMAGE','That image could not be decoded safely. Try another downloaded image.'));
    });
    try {child.send({bytes,info,maxBytes},error=>{if(error){failure=new MadlibError('IMAGE','The image could not be sent to the checker.');terminate();}});}
    catch (_) {failure=new MadlibError('IMAGE','The image could not be sent to the checker.');terminate();}
  });}finally{activeDecodes--;}
}
async function downloadAttachment(attachment,maxBytes=8388608,{fetcher=fetch,decode=normalizeImage}={}){
  const url=validateAttachment(attachment,maxBytes),abort=new AbortController();const timer=setTimeout(()=>abort.abort(),12000);let res;
  try{
    res=await fetcher(url,{signal:abort.signal,redirect:'manual',size:maxBytes,timeout:12000});check(res.status===200,'IMAGE_DOWNLOAD','Discord could not supply the attachment. Upload it again.');
    const contentLength=Number(res.headers.get('content-length')||0);check(Number.isFinite(contentLength)&&contentLength<=maxBytes,'IMAGE_SIZE','Image exceeds the upload limit.');
    const chunks=[];let total=0;for await(const chunk of res.body){total+=chunk.length;check(total<=maxBytes,'IMAGE_SIZE','Image exceeds the upload limit.');chunks.push(chunk);}
    // The declared raster MIME can be wrong; headers plus full decoding prove the type.
    const bytes=Buffer.concat(chunks),info=dimensions(bytes);return await decode(bytes,info,maxBytes);
  }catch(e){if(e instanceof MadlibError)throw e;throw new MadlibError('IMAGE_DOWNLOAD','The upload could not be downloaded safely. Attach the image again.');}
  finally{clearTimeout(timer);abort.abort();res?.body?.destroy?.();}
}
module.exports={dimensions,validateAttachment,normalizeImage,downloadAttachment};

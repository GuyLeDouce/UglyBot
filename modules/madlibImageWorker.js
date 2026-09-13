'use strict';
const {isMainThread,parentPort,workerData}=require('node:worker_threads');
if(!isMainThread){
  (async()=>{
    try{
      const {loadImage,createCanvas}=require('@napi-rs/canvas');
      const image=await loadImage(Buffer.from(workerData.bytes));
      if(image.width!==workerData.info.width||image.height!==workerData.info.height)throw Error('Dimensions changed');
      const canvas=createCanvas(image.width,image.height);canvas.getContext('2d').drawImage(image,0,0);
      const bytes=canvas.toBuffer('image/png');if(bytes.length>workerData.maxBytes)throw Error('Normalized image too large');
      parentPort.postMessage({ok:true,bytes});
    }catch(_){parentPort.postMessage({ok:false});}
  })();
}

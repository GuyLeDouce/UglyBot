'use strict';
// Run the native decoder outside the bot process. Never load index.js here.
if(require.main===module&&typeof process.send==='function'){
  process.once('message',async({bytes,info,maxBytes})=>{
    try{
      const {loadImage,createCanvas}=require('@napi-rs/canvas');
      const image=await loadImage(Buffer.from(bytes));
      const {width,height,media_type}=info;
      const same=image.width===width&&image.height===height;
      // Applying JPEG EXIF rotation can swap the encoded axes.
      const oriented=media_type==='image/jpeg'&&image.width===height&&image.height===width;
      if(!same&&!oriented)throw Error('Dimensions changed');
      const canvas=createCanvas(image.width,image.height);canvas.getContext('2d').drawImage(image,0,0);
      const normalized=canvas.toBuffer('image/png');
      if(normalized.length>maxBytes){process.send({ok:false,code:'IMAGE_SIZE'});return;}
      process.send({ok:true,bytes:normalized,width:image.width,height:image.height});
    }catch(_){process.send({ok:false});}
  });
}

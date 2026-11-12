'use strict';
// The active catalogue is the owner's 23 authored pairs. Saved sessions carry their
// own template snapshots, so retiring older catalogue entries never rewrites a play.
const stories=require('./data/madlibStoriesV7.json');
const examples={Place:'bowling alley',Adjective:'glittery',Noun:'rubber duck','Body Part':'elbow',
  'Plural Noun':'spatulas',Adverb:'dramatically',Number:'42',Food:'lasagna',Liquid:'lemonade',
  'Verb ending in -ing':'juggling','Past-tense Verb':'somersaulted',Exclamation:'Holy noodles',Verb:'dance'};
const hints={Place:'Name a location.',Adjective:'A word that describes something.',Noun:'Name a person, place or thing.',
  'Body Part':'Name a body part.','Plural Noun':'Name more than one thing.',Adverb:'Describe how an action happens, often ending in -ly.',
  Number:'Enter a number.',Food:'Name a food or dish.',Liquid:'Name a liquid.',
  'Verb ending in -ing':'An action ending in -ing.','Past-tense Verb':'An action that already happened.',
  Exclamation:'A short shout or expression; no quotation marks needed.',Verb:'An action in its base form.'};
function compile(text,words){
  return text.replace(/\[(\d+)\. ([A-Z -]+)\]/g,(_,n,type)=>{
    if(words[Number(n)-1]?.toUpperCase()!==type)throw Error(`Mismatched authored word prompt ${n}`);
    return `{{word_${n}}}`;
  });
}
module.exports=stories.map(s=>({
  id:`ugly-family-${String(s.number).padStart(2,'0')}`,version:7,promptFormat:'primary-reference',format:'authored-pair',enabled:true,
  squigCount:s.squigCount,imageSetup:`This image features ${s.squigCount} Squig${s.squigCount===1?'':'s'} total. Attach your Squig image as the primary character reference.`,
  title:s.title,category:'Ugly Family Adventures',
  storyTemplate:compile(s.story,s.wordPrompts),imageTemplate:compile(s.imagePrompt,s.wordPrompts),
  questions:s.wordPrompts.map((type,i)=>{
    if(!examples[type]||!hints[type])throw Error(`Unknown word type: ${type}`);
    return {key:`word_${i+1}`,label:type,type,hint:hints[type],example:examples[type],maxLength:60};
  }),
}));

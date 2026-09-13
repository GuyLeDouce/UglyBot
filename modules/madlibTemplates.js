'use strict';
// Static authored data, not runtime AI generation. Every exported entry contains
// complete ordered question definitions and one shared story/image scene contract.
const rows=[...require('./data/madlibScenesA'),...require('./data/madlibScenesB')];
const questions={
  emotion:['Emotion (adjective)','An emotion describing how someone feels.','furious',60],
  object:['Household object (singular noun)','One ordinary object; a short noun phrase.','coat hanger',60],
  food:['Food (noun or short phrase)','Name one food, not a list.','lasagna',60],
  animal:['Animal (singular noun)','Name an animal; it may become a small motif.','platypus',60],
  adjective:['Describing word (adjective)','A strange descriptive quality.','suspiciously elegant',60],
  colour:['Colour or colour phrase','One colour, without naming an object.','electric purple',60],
  plural:['Plural noun','More than one of something.','tiny umbrellas',60],
  costume:['Costume or outfit','Clothing only; deliberately layers over or replaces the original outfit.','oversized bathrobe',60],
  shout:['A ridiculous thing to shout','One short line of dialogue; no need for quotation marks.','Stay Ugly, Fkrs!',100],
};
const categories=['Morning / GM','Night / GN','Everyday errands','Workweek / office','Meme reactions','Web3 desk life','Food / coffee','Weekend / outdoors','Screens / posting','Social mischief'];
module.exports=rows.map(([id,title,theme,location,lead,moment,ending,composition,review,lettering=''])=>{
  // Use a neutral singular determiner before unknown answers: 'one orange cushion'
  // works for either vowel or consonant sounds without guessing a/an. This runs
  // on authored text only, before literal user substitution, and both outputs
  // snapshot the same normalized moment. User answers are never rewritten.
  moment=moment.replace(/\b(a|an) (?=\{\{)/gi,article=>article[0]==='A'?'One ':'one ');
  const keys=[...new Set([...moment.matchAll(/\{\{([a-z][a-z0-9_]*)\}\}/g)].map(m=>m[1]))];
  return {id,version:2,title,category:categories[theme],enabled:true,lead,ending,scene:{location,moment,composition,world:'human-world',lettering},review,
    questions:keys.map(key=>{if(!questions[key])throw new Error(`Unknown question key in static template ${id}`);const [label,hint,example,maxLength]=questions[key];return {key,label,type:key,hint,example,maxLength};})};
});

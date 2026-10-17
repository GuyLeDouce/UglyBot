'use strict';
// Static authored data, not runtime AI generation. Every exported entry contains
// complete ordered question definitions and one shared story/image scene contract.
const rows=[...require('./data/madlibScenesA'),...require('./data/madlibScenesB')];
const questions={
  emotion:['How badly are we handling this?','Give a feeling, not an action. Overreacting is encouraged.','unreasonably smug',60],
  object:['Pick a useless thing to bring','One household object. Imagine defending this choice to an adult.','decorative toilet brush',60],
  food:['What snack is worth losing dignity over?','One food or dish. This is not the time to impress a nutritionist.','emergency pocket lasagna',60],
  animal:['Pick an animal with questionable energy','One animal for a tiny design detail. No extra creature joins the scene.','judgmental goose',60],
  adjective:['Describe something deeply questionable','A describing word or phrase. Make it sound difficult to explain.','legally suspicious',60],
  colour:['Choose a colour with too much confidence','A colour or colour phrase, not an object. Subtlety is optional.','discount highlighter yellow',60],
  plural:['Name several things nobody ordered','A plural noun or phrase. These become a pattern, not a crowd.','tiny divorce papers',60],
  costume:['What outfit makes this decision worse?','Clothes only. Keep the Squig’s face, hair and original traits intact.','velvet bathrobe with shoulder pads',60],
  shout:['What is your completely useless defense?','One short line to say out loud. Skip quotation marks.','I was left unsupervised.',100],
};
const categories=['Morning / GM','Night / GN','Everyday errands','Workweek / office','Meme reactions','Web3 desk life','Food / coffee','Weekend / outdoors','Screens / posting','Social mischief'];
module.exports=rows.map(([id,title,theme,location,lead,moment,ending,composition,review,lettering=''])=>{
  // Use a neutral singular determiner before unknown answers: 'one orange cushion'
  // works for either vowel or consonant sounds without guessing a/an. This runs
  // on authored text only, before literal user substitution, and both outputs
  // snapshot the same normalized moment. User answers are never rewritten.
  moment=moment.replace(/\b(a|an) (?=\{\{)/gi,article=>article[0]==='A'?'One ':'one ');
  const keys=[...new Set([...moment.matchAll(/\{\{([a-z][a-z0-9_]*)\}\}/g)].map(m=>m[1]))];
  return {id,version:3,title,category:categories[theme],enabled:true,lead,ending,scene:{location,moment,composition,world:'human-world',lettering},review,
    questions:keys.map(key=>{if(!questions[key])throw new Error(`Unknown question key in static template ${id}`);const [label,hint,example,maxLength]=questions[key];return {key,label,type:key,hint,example,maxLength};})};
});

'use strict';
// Verify only Mad Libs' owned additions. The one-time installer deliberately keeps
// its historical whole-file hash guard and must not be run against newer index.js.
const {changes}=require('./installMadlibIntegration');
const additions=changes.map(([before,after])=>{
  let start=0,end=0;
  while(start<before.length&&before[start]===after[start])start++;
  while(end<before.length-start&&before[before.length-1-end]===after[after.length-1-end])end++;
  if(before.slice(start,before.length-end)!=='')throw new Error('Expected additive Mad Libs integration.');
  return after.slice(start,after.length-end);
});
function verify(source){
  let original=source;
  for(const addition of additions){
    if(original.split(addition).length!==2)throw new Error('Missing or duplicated Mad Libs integration addition.');
    original=original.replace(addition,'');
  }
  return original;
}
module.exports={verify,additions};

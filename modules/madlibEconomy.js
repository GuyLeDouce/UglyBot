'use strict';
const { MadlibError,check } = require('./madlibCore');
/** Opt-in guard called by the existing awardDripPoints helper only for Mad Libs.
 * One documented route, explicit currency, no alternative payload or credit fallback.
 * https://docs.drip.re/api-reference/realm-members-balances/transfer-member-balance-of-a-currency
 */
async function strictTransfer(realmId,memberIds,amount,currencyId,settings,options,transport) {
  const recipients=Array.isArray(memberIds)?memberIds:[memberIds];
  const sender=options.senderMemberIdOverride||transport.defaultSender;
  check(options.requireTransfer===true&&/^madlib_(play|reward|refund):/.test(options.context||''),'TRANSFER_CONFIG','Mad Libs must use a scoped transfer context.');
  check(recipients.length===1&&typeof recipients[0]==='string'&&recipients[0]&&sender&&sender!==recipients[0]&&sender!==transport.botDiscordId,'TRANSFER_CONFIG','A single unambiguous sender and recipient are required.');
  check(Number.isSafeInteger(amount)&&amount>0&&currencyId&&realmId&&settings?.drip_api_key,'TRANSFER_CONFIG','The amount, realm, currency, or existing DRIP credentials are not configured.');
  const url=`https://api.drip.re/api/v1/realms/${encodeURIComponent(realmId)}/members/${encodeURIComponent(sender)}/transfer`;
  let res;
  try {
    res=await transport.fetchWithTimeout(url,{timeoutMs:15000,timeout:15000,size:65536,redirect:'error',method:'PATCH',
      headers:transport.buildDripHeaders(settings,true),body:JSON.stringify({amount,recipientId:recipients[0],currencyId:String(currencyId)})});
  } catch (_) { throw new MadlibError('TRANSFER_UNCERTAIN','Payment outcome is unknown and needs admin review.'); }
  if(!res.ok) {
    res.body?.destroy?.();
    const definite=[400,401,403,404,422,429].includes(res.status);
    const retryAfter=Math.min(86400,Math.max(1,Number(res.headers?.get?.('retry-after'))||300));
    throw new MadlibError(definite?`DRIP_HTTP_${res.status}`:'TRANSFER_UNCERTAIN',
      definite?`DRIP rejected this transfer (HTTP ${res.status}). No fallback was attempted.`:'Payment outcome is unknown and needs admin review.',{safeToRetry:definite,retryAfter});
  }
  if(res.status===202){res.body?.destroy?.();throw new MadlibError('TRANSFER_UNCERTAIN','DRIP accepted the request without confirming settlement; admin review is required.');}
  let data={};
  try {data=await res.json();}catch(_){ /* A confirmed synchronous 2xx may omit its optional body. */ }
  if(data?.success===false||data?.error)throw new MadlibError('TRANSFER_UNCERTAIN','DRIP returned a conflicting success response; admin review is required.');
  // Check any echoed identity; a generic response id is not a proven transaction id.
  const receipt=data?.data&&typeof data.data==='object'?data.data:data;
  for(const [key,expected] of [['senderId',String(sender)],['recipientId',recipients[0]],['currencyId',String(currencyId)],['amount',amount]]) {
    if(receipt?.[key]!=null&&receipt[key]!==expected)throw new MadlibError('TRANSFER_UNCERTAIN','DRIP returned a mismatched transfer receipt; admin review is required.');
  }
  const ref=data?.transactionId||data?.data?.transactionId||null;
  return {usedMemberId:recipients[0],usedSenderId:String(sender),endpoint:'/transfer',method:'PATCH',transactionRef:typeof ref==='string'?ref.slice(0,200):null};
}
function canonicalMember(spendable,links,collectIds) {
  check(spendable?.ok,'IDENTITY','Your existing DRIP connection is not ready. Ask an admin to check verification.');
  const stored=[...new Set(links.filter(l=>l.verified&&l.drip_member_id).map(l=>String(l.drip_member_id).trim()))];
  const resolved=spendable.resolvedMember;
  const aliases=resolved?[...new Set(collectIds(resolved).map(String))]:[];
  if(resolved?.id) {
    check(stored.every(s=>aliases.includes(s))&&(spendable.memberIds||[]).every(s=>aliases.includes(String(s))),'IDENTITY_CONFLICT','Conflicting DRIP identities need admin review. No arbitrary recipient will be selected.');
    return String(resolved.id);
  }
  const ids=[...new Set((spendable.memberIds||[]).map(String))];
  check(stored.length===1&&ids.length===1&&stored[0]===ids[0],'IDENTITY_CONFLICT','A single verified DRIP member is required. Ask an admin to resolve the saved mapping.');return stored[0];
}
class MadlibEconomy {
  constructor(store,deps){this.store=store;this.deps=deps;}
  async resolve(op) {
    const d=this.deps;
    const [spendable,links]=await Promise.all([d.getMarketplaceSpendableBalance(op.guild_id,op.user_id),d.getWalletLinks(op.guild_id,op.user_id)]);
    const member=canonicalMember(spendable,links,d.collectDripMemberIdCandidates),settings=spendable.settings;
    check(String(settings.drip_realm_id)===op.realm_id&&String(settings.currency_id)===op.currency_id,'CURRENCY_CHANGED','The recorded realm or $CHARM currency differs from current configuration. Review the operation; do not switch currencies.');
    const bot=String(spendable.botMemberId||'');check(bot&&bot!==member&&bot!==d.clientUserId(),'IDENTITY_CONFLICT','The treasury and player must be distinct DRIP members, not Discord application IDs.');
    const sender=op.kind==='debit'?member:bot,recipient=op.kind==='debit'?bot:member;
    check((!op.sender_id||op.sender_id===sender)&&(!op.recipient_id||op.recipient_id===recipient),'IDENTITY_CHANGED','The saved sender or recipient changed. Admin review is required.');
    if(op.kind==='debit') {
      // Match Marketplace: parse the fresh resolved member before trying direct GETs.
      // Some DRIP versions return balances through member search, not /balance.
      check(typeof d.extractDripCurrencyAmountFromPayload==='function','BALANCE_CONFIG','The Marketplace balance parser was not connected. Deploy the complete Mad Libs fix.');
      const embedded=d.extractDripCurrencyAmountFromPayload(spendable.resolvedMember||null,op.currency_id);
      // canonicalMember above verified these aliases refer to the same account.
      const ids=[...new Set([member,...(spendable.memberIds||[]).map(String)])];
      const balance=embedded!=null?embedded:await d.getDripMemberCurrencyBalance(op.realm_id,ids,op.currency_id,settings);
      check(typeof balance==='number'&&Number.isFinite(balance)&&balance>=0,'BALANCE_UNKNOWN','Your $CHARM balance could not be confirmed. No charge was sent.');
      check(balance>=op.amount,'INSUFFICIENT_FUNDS',`You need ${op.amount.toLocaleString('en-US')} $CHARM. No charge was sent.`);
    }
    return {sender,recipient,settings};
  }
  async execute(opid=null) {
    const op=await this.store.claimOperation(opid);if(!op)return null;let armed=null;
    try {
      const identity=await this.resolve(op);armed=await this.store.armOperation(op,identity);if(!armed)return null;
      const result=await this.deps.awardDripPoints(op.realm_id,[identity.recipient],op.amount,op.currency_id,identity.settings,{
        context:op.id,initiatorDiscordId:op.user_id,recipientDiscordId:op.kind==='debit'?this.deps.clientUserId():op.user_id,
        recipientMemberIdOverride:identity.recipient,senderMemberIdOverride:identity.sender,requireTransfer:true,madlibStrictTransfer:true});
      check(result?.usedMemberId===identity.recipient&&result?.usedSenderId===identity.sender,'TRANSFER_UNCERTAIN','Transfer receipt identity did not match the recorded operation.');
      // Failure after remote success is NOT permission to send again.
      await this.store.finishOperation(armed,result.transactionRef||null);return {state:'confirmed_success',operationId:op.id};
    } catch(error) {
      const code=error instanceof MadlibError?error.code:(armed?'TRANSFER_UNCERTAIN':'PREFLIGHT_UNAVAILABLE');
      const uncertain=Boolean(armed)&&(!error.safeToRetry||code==='TRANSFER_UNCERTAIN');
      const preflightRecoverable=['IDENTITY','BALANCE_UNKNOWN','PREFLIGHT_UNAVAILABLE','PAUSED'].includes(code);
      const retry=!uncertain&&((!armed&&preflightRecoverable&&op.kind!=='debit')||(code==='DRIP_HTTP_429'&&(armed?.attempt_count||0)<5));
      const backoff=Math.min(3600,60*(2**Math.min(6,Math.floor(op.revision/2)))),jitter=Math.floor((this.deps.random||Math.random)()*30);
      await this.store.failOperation(armed||op,code,{uncertain,retry,delaySeconds:Math.max(error.retryAfter||0,backoff)+jitter}).catch(()=>{ /* Durable state remains for stale recovery/review. */ });
      if(uncertain||!retry)await this.deps.postAdminSystemLog({guildId:op.guild_id,category:'Mad Lib Payment',message:`Operation: ${op.id}\nState: ${uncertain?'needs_review':'confirmed_failure'}\nCode: ${code}\nNo automatic resend of uncertain transfers. Use /madlib-admin inspect.`}).catch(()=>{});
      return {state:uncertain?'needs_review':retry?'retryable_failure':'confirmed_failure',operationId:op.id,code};
    }
  }
}
module.exports={strictTransfer,canonicalMember,MadlibEconomy};

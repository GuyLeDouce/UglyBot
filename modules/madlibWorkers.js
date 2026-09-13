'use strict';
const {Events}=require('discord.js');
const {reactorAccess}=require('./madlibAccess');
/** Gateway and paginated reconciliation share one database-unique award path. */
class MadlibWorkers {
  constructor({deps,store,economy,publishing,cfg,log=()=>{},timers=globalThis}){
    Object.assign(this,{deps,store,economy,publishing,cfg,log,timers});this.running=false;this.busy=false;this.queue=[];
    this.routes=new Set();this.channels=new Set();this.routesAt=0;this.raw=p=>this.enqueue(p);
  }
  start(){
    if(this.running)return false;this.running=true;this.deps.client.on(Events.Raw,this.raw);
    this.timer=this.timers.setInterval(()=>this.tick().catch(()=>this.log('WORKER_FAILURE')),5000);this.timer?.unref?.();void this.tick().catch(()=>this.log('WORKER_FAILURE'));return true;
  }
  stop(){this.running=false;if(this.timer)this.timers.clearInterval(this.timer);this.deps.client.off(Events.Raw,this.raw);this.queue.length=0;}
  remember(p){this.routes.add(`${p.guild_id}:${p.channel_id}:${p.emoji_id}`);this.channels.add(`${p.guild_id}:${p.channel_id}`);}
  async refreshRoutes(){
    if(Date.now()-this.routesAt<60000)return;
    const rows=(await this.store.query("SELECT DISTINCT guild_id,channel_id,emoji_id FROM madlib_publications WHERE status='published'")).rows;
    this.routes.clear();this.channels.clear();for(const p of rows)this.remember(p);this.routesAt=Date.now();
  }
  enqueue(packet){
    if(!this.running)return false;const d=packet?.d;if(!d?.guild_id||!d.channel_id||!d.message_id)return false;
    if(!this.channels.has(`${d.guild_id}:${d.channel_id}`))return false;
    if(!['MESSAGE_REACTION_ADD','MESSAGE_DELETE','MESSAGE_REACTION_REMOVE_ALL','MESSAGE_REACTION_REMOVE_EMOJI'].includes(packet.t))return false;
    if(packet.t==='MESSAGE_REACTION_ADD'&&(!d.emoji?.id||!d.user_id||d.user_id===this.deps.clientUserId()||!this.routes.has(`${d.guild_id}:${d.channel_id}:${d.emoji.id}`)))return false;
    if(this.queue.length>=500){this.log('RAW_QUEUE_FULL_RECONCILIATION_WILL_RECOVER');return false;}
    this.queue.push({t:packet.t,guild:d.guild_id,channel:d.channel_id,message:d.message_id,emoji:d.emoji?.id,user:d.user_id});return true;
  }
  async candidate(p,userId){
    if(!this.running||p.suspended)return null;
    if(await this.store.one('SELECT 1 FROM madlib_reactions WHERE publication_id=$1 AND reactor_id=$2 AND emoji_id=$3',[p.id,String(userId),p.emoji_id]))return null;
    return await reactorAccess(this.deps,p,String(userId))?this.store.recordReaction(p,String(userId)):null;
  }
  async gateway(event){
    const p=await this.store.publicationByMessage(event.guild,event.channel,event.message,event.t==='MESSAGE_REACTION_ADD'?event.emoji:null);if(!p)return;
    if(event.t==='MESSAGE_DELETE'){await this.store.query("UPDATE madlib_publications SET status='deleted',moderation_reason='DISCORD_MESSAGE_DELETED',revision=revision+1,updated_at=now() WHERE id=$1 AND status='published'",[p.id]);return;}
    if(event.t!=='MESSAGE_REACTION_ADD'){await this.store.query('UPDATE madlib_publications SET emoji_seeded=FALSE,next_reconcile_at=now() WHERE id=$1',[p.id]);return;}
    const channel=await this.deps.client.channels.fetch(p.channel_id),message=await channel.messages.fetch({message:p.message_id,force:true,cache:false});
    if(message.author?.id!==this.deps.clientUserId()||message.guildId!==p.guild_id)return;await this.candidate(p,event.user);
  }
  async reconcile(){
    const p=await this.store.claimReconcile();if(!p)return;let release={delay:this.cfg.MADLIB_RECONCILE_INTERVAL_SECONDS};
    try{
      const {channel,emoji}=await this.publishing.target(p.guild_id,p.channel_id,p.emoji_id);
      if(p.status==='needs_review'){const found=await this.publishing.reconcileUncertain(p,channel);if(found?.found){this.remember(p);return;}release={...release,...found};return;}
      const message=await channel.messages.fetch({message:p.message_id,force:true,cache:false});
      if(message.author?.id!==this.deps.clientUserId()||message.guildId!==p.guild_id){release={deleted:true,delay:86400};this.log('PUBLICATION_AUTHOR_OR_GUILD_MISMATCH');return;}
      if(!p.emoji_seeded)await this.publishing.seed(p.id,message,emoji);
      await this.publishing.refresh(p,message);if(p.suspended)return;
      const reaction=message.reactions.cache.find(r=>r.emoji.id===p.emoji_id);if(!reaction)return;
      // Verified discord.js 14.16.3 supports explicit normal/burst user pagination.
      const users=await reaction.users.fetch({type:p.reconcile_type,limit:100,...(p.reconcile_after?{after:p.reconcile_after}:{})});
      for(const user of users.values()){if(!this.running)break;if(!user.bot&&!user.system)await this.candidate(p,user.id);}
      const keys=[...users.keys()].sort((a,b)=>BigInt(a)<BigInt(b)?-1:1);
      if(users.size===100)release={after:keys.at(-1),type:p.reconcile_type,delay:5};
      else if(p.reconcile_type===0)release={after:null,type:1,delay:5};
      else release={after:null,type:0,delay:this.cfg.MADLIB_RECONCILE_INTERVAL_SECONDS};
    }catch(e){if(Number(e.code)===10008||Number(e.code)===10003)release={deleted:true,delay:86400};else{this.log('RECONCILE_FAILED');release.delay=Math.max(300,this.cfg.MADLIB_RECONCILE_INTERVAL_SECONDS);}}
    finally{await this.store.releaseReconcile(p,release);}
  }
  async tick(){
    if(!this.running||this.busy)return;this.busy=true;
    try{
      await this.store.recoverStale();await this.refreshRoutes();
      for(let n=0;this.running&&n<10&&this.queue.length;n++){const event=this.queue.shift();try{await this.gateway(event);}catch(_){this.log('RAW_CANDIDATE_DEFERRED_TO_RECONCILIATION');}}
      if(this.running)await this.economy.execute();for(let n=0;this.running&&n<2;n++)await this.reconcile();
    }finally{this.busy=false;}
  }
}
module.exports={MadlibWorkers};

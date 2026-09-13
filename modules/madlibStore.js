'use strict';
const fs = require('node:fs');
const path = require('node:path');
const core = require('./madlibCore');
const { check, id, selectTemplate, render, validateAnswer } = core;
const ACTIVE = ['active','awaiting_payment','payment_failed','payment_review','refund_pending'];
class MadlibStore {
  constructor(pool, { random = Math.random } = {}) { this.pool = pool; this.random = random; }
  async query(sql, params = []) { return this.pool.query({text:sql,values:params,query_timeout:10000}); }
  async one(sql, params = [], db = this.pool) { return (await db.query({text:sql,values:params,query_timeout:10000})).rows[0] || null; }
  async acquire() {
    let expired=false,timer;
    try{return await Promise.race([
      this.pool.connect().then(c=>{if(expired){c.release();throw new core.MadlibError('DATABASE_BUSY','Mad Lib database is busy. Try Resume shortly.');}return c;}),
      new Promise((_,reject)=>{timer=setTimeout(()=>{expired=true;reject(new core.MadlibError('DATABASE_BUSY','Mad Lib database is busy. Try Resume shortly.'));},5000);}),
    ]);}finally{clearTimeout(timer);}
  }
  async tx(fn) {
    const c=await this.acquire();let discard=false;
    try { await c.query({text:'BEGIN',query_timeout:5000});await c.query("SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='3s'");const result=await fn(c);await c.query({text:'COMMIT',query_timeout:5000});return result; }
    catch(e){await c.query({text:'ROLLBACK',query_timeout:5000}).catch(()=>{discard=true;});throw e;}finally{c.release(discard);}
  }
  async ensureMadlibTables() {
    await this.tx(async c=>{
      await c.query("SELECT pg_advisory_xact_lock(hashtext('uglybot:madlib:schema:v1'))");
      await c.query(fs.readFileSync(path.join(__dirname,'madlibSchema.sql'),'utf8'));
      check((await this.one('SELECT version FROM madlib_schema WHERE singleton=TRUE',[],c)).version===1,'SCHEMA','Mad Libs schema version differs from this build.');
    });
  }
  async lockUser(c,guild,user) {
    await c.query('INSERT INTO madlib_guilds(guild_id) VALUES($1) ON CONFLICT DO NOTHING',[guild]);
    await c.query('INSERT INTO madlib_users(guild_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING',[guild,user]);
    return this.one('SELECT *,clock_timestamp() AS db_now FROM madlib_users WHERE guild_id=$1 AND user_id=$2 FOR UPDATE',[guild,user],c);
  }
  async owned(guild,user,sid,c=this.pool,lock=false) {
    const s=await this.one(`SELECT * FROM madlib_sessions WHERE id=$1 AND guild_id=$2 AND user_id=$3${lock?' FOR UPDATE':''}`,[sid,guild,user],c);
    check(s,'OWNER','That saved story is not yours in this server.');return s;
  }
  async active(guild,user){return this.one('SELECT * FROM madlib_sessions WHERE guild_id=$1 AND user_id=$2 AND state=ANY($3::text[])',[guild,user,ACTIVE]);}
  async begin({guild,user,name,templates,cfg,quoteId=null,settings=null}) {
    return this.tx(async c=>{
      const u=await this.lockUser(c,guild,user);
      const existing=await this.one('SELECT * FROM madlib_sessions WHERE guild_id=$1 AND user_id=$2 AND state=ANY($3::text[])',[guild,user,ACTIVE],c);
      if(existing)return {session:existing,resumed:true};
      const free=u.db_now>=u.next_free_at;
      if(!free){
        const q=quoteId&&await this.one('SELECT * FROM madlib_quotes WHERE id=$1 AND guild_id=$2 AND user_id=$3',[quoteId,guild,user],c);
        if(!q||q.expires_at<=u.db_now||q.amount!==cfg.MADLIB_EXTRA_PLAY_COST_CHARM){
          const quote=await this.one(`INSERT INTO madlib_quotes(id,guild_id,user_id,amount,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+interval '5 minutes')
            ON CONFLICT(guild_id,user_id) DO UPDATE SET id=EXCLUDED.id,amount=EXCLUDED.amount,expires_at=EXCLUDED.expires_at RETURNING *`,[id(),guild,user,cfg.MADLIB_EXTRA_PLAY_COST_CHARM],c);
          return {quote,nextFreeAt:u.next_free_at};
        }
        check(settings?.drip_realm_id&&settings?.currency_id,'CONFIG','Paid play is unavailable until the existing DRIP realm and $CHARM currency are configured.');
        check(!(await this.one('SELECT finances_paused FROM madlib_guilds WHERE guild_id=$1',[guild],c)).finances_paused,'PAUSED','Mad Lib payments are paused. Free plays, history and already-paid progress remain available.');
      }
      const recent=(await c.query("SELECT template_id FROM madlib_sessions WHERE guild_id=$1 AND user_id=$2 AND state='completed' ORDER BY completed_at DESC,id LIMIT 10",[guild,user])).rows.map(x=>x.template_id);
      const template=selectTemplate(templates,recent,this.random),sid=id();
      const s=await this.one(`INSERT INTO madlib_sessions(id,guild_id,user_id,display_name,template_id,template_version,template,state,cost,previous_free_at,reserved_free_until)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,[sid,guild,user,core.displayName(name),template.id,template.version,JSON.stringify(template),free?'active':'awaiting_payment',free?0:cfg.MADLIB_EXTRA_PLAY_COST_CHARM,free?u.next_free_at:null,free?new Date(u.db_now.getTime()+cfg.MADLIB_FREE_COOLDOWN_HOURS*3600000):null],c);
      await c.query('UPDATE madlib_users SET active_session_id=$3,next_free_at=COALESCE($4,next_free_at) WHERE guild_id=$1 AND user_id=$2',[guild,user,sid,s.reserved_free_until]);
      if(!free)await c.query(`INSERT INTO madlib_operations(id,guild_id,user_id,kind,amount,realm_id,currency_id,session_id) VALUES($1,$2,$3,'debit',$4,$5,$6,$7)`,[`madlib_play:${sid}`,guild,user,s.cost,String(settings.drip_realm_id),String(settings.currency_id),sid]);
      await c.query('DELETE FROM madlib_quotes WHERE guild_id=$1 AND user_id=$2',[guild,user]);return {session:s,resumed:false};
    });
  }
  async deliveryAttempt(guild,user,sid){check(await this.one("UPDATE madlib_sessions SET delivery_attempts=delivery_attempts+1 WHERE id=$1 AND guild_id=$2 AND user_id=$3 AND state='active' RETURNING id",[sid,guild,user]),'STALE','That session changed. Use PLAY / Resume.');}
  async delivered(guild,user,sid){return this.query('UPDATE madlib_sessions SET delivered_at=COALESCE(delivered_at,now()) WHERE id=$1 AND guild_id=$2 AND user_id=$3',[sid,guild,user]);}
  async restoreUndeliveredFree(guild,user,sid){
    return this.tx(async c=>{
      await this.lockUser(c,guild,user);const s=await this.owned(guild,user,sid,c,true);
      if(s.cost||s.delivered_at||s.delivery_attempts>1||s.state!=='active'||Object.keys(s.answers).length)return false;
      await c.query("UPDATE madlib_sessions SET state='failed',revision=revision+1,updated_at=now() WHERE id=$1",[sid]);
      await c.query('UPDATE madlib_users SET next_free_at=CASE WHEN next_free_at=$4 THEN $3 ELSE next_free_at END,active_session_id=NULL WHERE guild_id=$1 AND user_id=$2 AND active_session_id=$5',[guild,user,s.previous_free_at,s.reserved_free_until,sid]);return true;
    });
  }
  async editSession(guild,user,sid,revision,action,value){
    return this.tx(async c=>{
      await this.lockUser(c,guild,user);const s=await this.owned(guild,user,sid,c,true);
      check(s.revision===revision,'STALE','This question changed elsewhere. Reopen Resume for the latest saved progress.');
      if(action==='abandon'){
        check(['active','payment_failed'].includes(s.state),'PAYMENT','Pending or uncertain payments must be reviewed before abandoning this play.');
        await c.query("UPDATE madlib_sessions SET state='cancelled',revision=revision+1,updated_at=now() WHERE id=$1",[sid]);
        await c.query('UPDATE madlib_users SET active_session_id=NULL WHERE guild_id=$1 AND user_id=$2 AND active_session_id=$3',[guild,user,sid]);return {...s,state:'cancelled',revision:s.revision+1};
      }
      check(s.state==='active','STATE','This session is not ready for answers. Resume to see its payment status.');
      let step=s.step;const answers={...s.answers};let output=null;
      if(action==='back')step=Math.max(0,step-1);
      else{check(action==='answer'&&s.template.questions[step],'STATE','No active question was found.');const q=s.template.questions[step];answers[q.key]=validateAnswer(value,q);step++;if(step===s.template.questions.length)output=render(s.template,answers);}
      const row=await this.one(`UPDATE madlib_sessions SET answers=$2,step=$3,revision=revision+1,state=$4,story=$5,prompt=$6,
        completed_at=CASE WHEN $4='completed' THEN now() ELSE NULL END,updated_at=now() WHERE id=$1 RETURNING *`,[sid,JSON.stringify(answers),step,output?'completed':'active',output?.story||null,output?.prompt||null],c);
      if(output)await c.query('UPDATE madlib_users SET active_session_id=NULL WHERE guild_id=$1 AND user_id=$2 AND active_session_id=$3',[guild,user,sid]);return row;
    });
  }
  async history(guild,user,page=0){
    check(Number.isSafeInteger(page)&&page>=0&&page<=1000000,'PAGE','Invalid history page.');
    return (await this.query(`SELECT s.*,p.id AS publication_id,p.status AS publication_status,p.channel_id,p.message_id
      FROM madlib_sessions s LEFT JOIN madlib_publications p ON p.session_id=s.id
      WHERE s.guild_id=$1 AND s.user_id=$2 AND s.state='completed' ORDER BY s.completed_at DESC,s.id DESC LIMIT 21 OFFSET $3`,[guild,user,page*20])).rows;
  }
  async chooseDraft(guild,user,sid){
    return this.tx(async c=>{
      const s=await this.owned(guild,user,sid,c,true);check(s.state==='completed','STATE','Finish the story first.');
      const pub=await this.one('SELECT * FROM madlib_publications WHERE session_id=$1 FOR UPDATE',[sid],c);
      if(pub&&pub.status!=='prepared')return {session:s,publication:pub};
      const d=await this.one(`INSERT INTO madlib_drafts(guild_id,user_id,session_id) VALUES($1,$2,$3)
        ON CONFLICT(guild_id,user_id) DO UPDATE SET session_id=$3,revision=madlib_drafts.revision+1,updated_at=now() RETURNING *`,[guild,user,sid],c);
      const image=await this.one('UPDATE madlib_uploads SET revision=$2 WHERE session_id=$1 AND expires_at>now() RETURNING revision',[sid,d.revision],c);
      if(pub&&image)await c.query('UPDATE madlib_publications SET upload_revision=$2,revision=revision+1,updated_at=now() WHERE id=$1',[pub.id,d.revision]);
      return {session:s,draft:d};
    });
  }
  async draft(guild,user){return this.one('SELECT * FROM madlib_drafts WHERE guild_id=$1 AND user_id=$2',[guild,user]);}
  async stage(guild,user,sid,revision,image){
    return this.tx(async c=>{
      const s=await this.owned(guild,user,sid,c,true);check(s.state==='completed','STATE','Finish the story first.');
      const publication=await this.one('SELECT * FROM madlib_publications WHERE session_id=$1 FOR UPDATE',[sid],c);
      check(!publication||publication.status==='prepared','PUBLISHED','A sent or uncertain publication already exists. Use SHOW for its status.');
      const d=await this.one('SELECT * FROM madlib_drafts WHERE guild_id=$1 AND user_id=$2 FOR UPDATE',[guild,user],c);
      check(d?.session_id===sid&&d.revision===revision,'STALE','Your SHOW selection changed. Select the story again before uploading.');const next=d.revision+1;
      await c.query(`INSERT INTO madlib_uploads(session_id,bytes,media_type,width,height,revision) VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT(session_id) DO UPDATE SET bytes=$2,media_type=$3,width=$4,height=$5,revision=$6,expires_at=now()+interval '24 hours'`,[sid,image.bytes,image.media_type,image.width,image.height,next]);
      await c.query('UPDATE madlib_drafts SET revision=$3,updated_at=now() WHERE guild_id=$1 AND user_id=$2',[guild,user,next]);
      if(publication)await c.query('UPDATE madlib_publications SET upload_revision=$2,revision=revision+1,updated_at=now() WHERE id=$1',[publication.id,next]);return {...d,revision:next};
    });
  }
  async upload(sid){return this.one('SELECT * FROM madlib_uploads WHERE session_id=$1 AND expires_at>now()',[sid]);}
  async cancelUpload(guild,user,sid,revision){
    return this.tx(async c=>{
      await this.owned(guild,user,sid,c,true);const d=await this.one('SELECT * FROM madlib_drafts WHERE guild_id=$1 AND user_id=$2 FOR UPDATE',[guild,user],c);
      check(d?.session_id===sid&&d.revision===revision,'STALE','The preview changed. Open SHOW again.');
      const p=await this.one('SELECT * FROM madlib_publications WHERE session_id=$1 FOR UPDATE',[sid],c);check(!p||p.status==='prepared','PUBLISHED','A sent or uncertain publication already exists. Use SHOW for its status.');
      await c.query('DELETE FROM madlib_uploads WHERE session_id=$1',[sid]);await c.query('DELETE FROM madlib_drafts WHERE guild_id=$1 AND user_id=$2',[guild,user]);
    });
  }
  async publicationIntent(guild,user,sid,revision,cfg,settings){
    return this.tx(async c=>{
      const s=await this.owned(guild,user,sid,c,true);check(s.state==='completed','STATE','Finish the story first.');
      const existing=await this.one('SELECT * FROM madlib_publications WHERE session_id=$1',[sid],c);if(existing)return existing;
      const d=await this.one('SELECT * FROM madlib_drafts WHERE guild_id=$1 AND user_id=$2 FOR UPDATE',[guild,user],c);
      const upload=await this.one('SELECT * FROM madlib_uploads WHERE session_id=$1 AND expires_at>now()',[sid],c);
      check(d?.session_id===sid&&d.revision===revision&&upload?.revision===revision,'STALE','The preview changed or expired. Upload again through SHOW.');
      check(settings?.currency_id&&settings?.drip_realm_id,'CONFIG','Configure the existing DRIP realm and $CHARM currency before publishing rewards.');
      return this.one(`INSERT INTO madlib_publications(id,session_id,guild_id,user_id,channel_id,status,filename,emoji_id,realm_id,currency_id,reward,reward_cap,reactor_roles,upload_revision)
        VALUES($1,$2,$3,$4,$5,'prepared','madlib-image.png',$6,$7,$8,$9,$10,$11,$12) RETURNING *`,[id(),sid,guild,user,cfg.MADLIB_CHANNEL_ID,cfg.MADLIB_REACTION_EMOJI_ID,String(settings.drip_realm_id),String(settings.currency_id),cfg.MADLIB_REACTION_REWARD_CHARM,cfg.MADLIB_REWARD_CAP_PER_POST,JSON.stringify(cfg.MADLIB_REACTOR_ROLE_IDS),revision],c);
    });
  }
  async claimPublication(pid,revision){return this.one("UPDATE madlib_publications SET status='publishing',lease_id=$2,lease_until=now()+interval '2 minutes',revision=revision+1,updated_at=now() WHERE id=$1 AND status='prepared' AND revision=$3 RETURNING *",[pid,id(),revision]);}
  async publication(pid){return this.one('SELECT * FROM madlib_publications WHERE id=$1',[pid]);}
  async publicationByMessage(guild,channel,message,emoji=null){return this.one("SELECT * FROM madlib_publications WHERE guild_id=$1 AND channel_id=$2 AND message_id=$3 AND status='published' AND ($4::text IS NULL OR emoji_id=$4)",[guild,channel,message,emoji]);}
  async confirmPublication(pid,message,leaseId=null,review=null){
    return this.tx(async c=>{
      const p=await this.one('SELECT * FROM madlib_publications WHERE id=$1 FOR UPDATE',[pid],c);
      check(p&&['publishing','needs_review','published'].includes(p.status),'STATE','Publication is not awaiting confirmation.');check(!p.message_id||p.message_id===message.id,'CONFLICT','Another message is already recorded for this story.');
      if(leaseId)check(p.lease_id===leaseId,'STALE','Publication worker changed.');
      if(review){check(p.guild_id===review.guild&&p.revision===review.revision&&p.status==='needs_review','STALE','Publication review changed; inspect again.');await this.audit(c,review.guild,review.actor,'link-publication',pid,review.evidence,{messageId:message.id});}
      const a=message.attachments?.first?.()||[...(message.attachments?.values?.()||[])][0];
      await c.query("UPDATE madlib_publications SET status='published',message_id=$2,attachment_id=$3,emoji_seeded=FALSE,lease_until=NULL,lease_id=NULL,revision=revision+1,updated_at=now(),next_reconcile_at=now() WHERE id=$1",[pid,message.id,a?.id||null]);
      await c.query('DELETE FROM madlib_uploads WHERE session_id=$1',[p.session_id]);
    });
  }
  async reviewPublication(pid,code){return this.query("UPDATE madlib_publications SET status='needs_review',moderation_reason=$2,lease_until=NULL,revision=revision+1,updated_at=now(),next_reconcile_at=now() WHERE id=$1 AND status='publishing'",[pid,code]);}
  async recordReaction(p,reactor){
    return this.tx(async c=>{
      const pub=await this.one('SELECT * FROM madlib_publications WHERE id=$1 FOR UPDATE',[p.id],c);
      if(!pub||pub.status!=='published'||pub.suspended||pub.user_id===reactor)return null;
      if(await this.one('SELECT operation_id FROM madlib_reactions WHERE publication_id=$1 AND reactor_id=$2 AND emoji_id=$3',[p.id,reactor,pub.emoji_id],c))return null;
      if(pub.reward_cap){const total=await this.one('SELECT COALESCE(SUM(amount),0)::bigint AS total FROM madlib_reactions WHERE publication_id=$1',[p.id],c);if(Number(total.total)+pub.reward>pub.reward_cap)return null;}
      const op=`madlib_reward:${p.id}:${reactor}:${pub.emoji_id}`;
      await c.query(`INSERT INTO madlib_operations(id,guild_id,user_id,kind,amount,realm_id,currency_id,publication_id,reactor_id) VALUES($1,$2,$3,'reward',$4,$5,$6,$7,$8)`,[op,pub.guild_id,pub.user_id,pub.reward,pub.realm_id,pub.currency_id,pub.id,reactor]);
      await c.query('INSERT INTO madlib_reactions(publication_id,reactor_id,emoji_id,amount,realm_id,currency_id,operation_id) VALUES($1,$2,$3,$4,$5,$6,$7)',[pub.id,reactor,pub.emoji_id,pub.reward,pub.realm_id,pub.currency_id,op]);
      await c.query('UPDATE madlib_publications SET display_dirty=TRUE WHERE id=$1',[pub.id]);return op;
    });
  }
  async totals(pid){return this.one(`SELECT COUNT(*)::integer AS eligible,COALESCE(SUM(amount) FILTER(WHERE state='confirmed_success'),0)::bigint AS paid,
    COALESCE(SUM(amount) FILTER(WHERE state<>'confirmed_success'),0)::bigint AS pending,
    COALESCE(SUM(amount) FILTER(WHERE state='needs_review'),0)::bigint AS review FROM madlib_operations WHERE publication_id=$1 AND kind='reward'`,[pid]);}
  async recoverStale(){
    await this.query("UPDATE madlib_operations SET state='prepared',lease_id=NULL,lease_until=NULL,revision=revision+1 WHERE state='resolving' AND lease_until<now()");
    await this.tx(async c=>{const rows=(await c.query("UPDATE madlib_operations SET state='needs_review',error_code='STALE_IN_FLIGHT',revision=revision+1,updated_at=now() WHERE state='in_flight' AND lease_until<now() RETURNING session_id,kind")).rows;for(const o of rows)if(o.kind==='debit')await c.query("UPDATE madlib_sessions SET state='payment_review',revision=revision+1 WHERE id=$1 AND state='awaiting_payment'",[o.session_id]);});
    await this.query("UPDATE madlib_publications SET status='needs_review',revision=revision+1,next_reconcile_at=now() WHERE status='publishing' AND lease_until<now()");
    await this.query("DELETE FROM madlib_uploads u WHERE expires_at<now() AND NOT EXISTS(SELECT 1 FROM madlib_publications p WHERE p.session_id=u.session_id AND p.status IN ('publishing','needs_review'))");
  }
  async claimOperation(opid=null){
    return this.tx(async c=>{
      const o=await this.one(`SELECT o.* FROM madlib_operations o JOIN madlib_guilds g USING(guild_id)
        WHERE o.state IN ('prepared','retryable_failure') AND o.next_attempt_at<=now() AND NOT g.finances_paused
        AND ($1::text IS NULL OR o.id=$1) ORDER BY o.next_attempt_at,o.created_at FOR UPDATE OF o SKIP LOCKED LIMIT 1`,[opid],c);
      if(!o)return null;return this.one("UPDATE madlib_operations SET state='resolving',lease_id=$2,lease_until=now()+interval '2 minutes',revision=revision+1,updated_at=now() WHERE id=$1 RETURNING *",[o.id,id()],c);
    });
  }
  async armOperation(op,identity){
    return this.tx(async c=>{
      const o=await this.one('SELECT * FROM madlib_operations WHERE id=$1 FOR UPDATE',[op.id],c);if(!o||o.state!=='resolving'||o.lease_id!==op.lease_id)return null;
      const g=await this.one('SELECT finances_paused FROM madlib_guilds WHERE guild_id=$1',[o.guild_id],c);check(!g.finances_paused,'PAUSED','Mad Lib payments are paused.');
      check((!o.sender_id||o.sender_id===identity.sender)&&(!o.recipient_id||o.recipient_id===identity.recipient),'IDENTITY_CHANGED','The saved payment identity changed; admin review is required.');
      return this.one("UPDATE madlib_operations SET state='in_flight',sender_id=$3,recipient_id=$4,attempt_count=attempt_count+1,sent_at=now(),lease_until=now()+interval '2 minutes',revision=revision+1,updated_at=now() WHERE id=$1 AND lease_id=$2 RETURNING *",[o.id,op.lease_id,identity.sender,identity.recipient],c);
    });
  }
  async failOperation(op,code,{uncertain=false,retry=false,delaySeconds=300}={}){
    return this.tx(async c=>{
      const state=uncertain?'needs_review':retry?'retryable_failure':'confirmed_failure';
      const o=await this.one(`UPDATE madlib_operations SET state=$3,error_code=$4,next_attempt_at=now()+($5::integer*interval '1 second'),revision=revision+1,updated_at=now(),lease_until=NULL
        WHERE id=$1 AND lease_id=$2 AND state IN ('resolving','in_flight','needs_review') RETURNING *`,[op.id,op.lease_id,state,code,Math.min(86400,Math.max(1,delaySeconds))],c);
      if(o?.kind==='debit')await c.query("UPDATE madlib_sessions SET state=$2,revision=revision+1,updated_at=now() WHERE id=$1 AND state IN ('awaiting_payment','payment_review','payment_failed')",[o.session_id,uncertain?'payment_review':retry?'awaiting_payment':'payment_failed']);
      if(o?.publication_id)await c.query('UPDATE madlib_publications SET display_dirty=TRUE WHERE id=$1',[o.publication_id]);return o;
    });
  }
  async finishOperation(op,providerRef=null,c=null){
    if(!c)return this.tx(db=>this.finishOperation(op,providerRef,db));
    const current=await this.one('SELECT * FROM madlib_operations WHERE id=$1 FOR UPDATE',[op.id],c);if(current?.state==='confirmed_success')return current;
    check(current&&current.lease_id===op.lease_id&&['in_flight','needs_review'].includes(current.state),'STALE','Payment ownership changed; admin review is required.');
    const o=await this.one("UPDATE madlib_operations SET state='confirmed_success',provider_ref=$2,confirmed_at=now(),lease_until=NULL,error_code=NULL,revision=revision+1,updated_at=now() WHERE id=$1 RETURNING *",[op.id,providerRef],c);
    if(o.kind==='debit')await c.query("UPDATE madlib_sessions SET state='active',revision=revision+1,updated_at=now() WHERE id=$1 AND state IN ('awaiting_payment','payment_failed','payment_review')",[o.session_id]);
    if(o.kind==='refund'){await c.query("UPDATE madlib_sessions SET state='refunded',revision=revision+1,updated_at=now() WHERE id=$1",[o.session_id]);await c.query('UPDATE madlib_users SET active_session_id=NULL WHERE guild_id=$1 AND user_id=$2 AND active_session_id=$3',[o.guild_id,o.user_id,o.session_id]);}
    if(o.publication_id)await c.query('UPDATE madlib_publications SET display_dirty=TRUE WHERE id=$1',[o.publication_id]);return o;
  }
  async operation(opid,guild=null){return this.one('SELECT * FROM madlib_operations WHERE id=$1 AND ($2::text IS NULL OR guild_id=$2)',[opid,guild]);}
  async audit(c,guild,actor,action,record,evidence,detail={}){await c.query('INSERT INTO madlib_audit(guild_id,actor_id,action,record_id,evidence,detail) VALUES($1,$2,$3,$4,$5,$6)',[guild,actor,action,record,evidence,JSON.stringify(detail)]);}
  async admin({guild,actor,action,record,revision,evidence}){
    check(evidence?.trim().length>=8&&evidence.length<=500,'EVIDENCE','Provide a specific reason or transaction evidence (8–500 characters).');
    return this.tx(async c=>{
      let result;
      if(['pause','resume'].includes(action)){
        const g=await this.one('SELECT * FROM madlib_guilds WHERE guild_id=$1 FOR UPDATE',[guild],c);check(g&&g.revision===revision,'STALE','Run status again and use the latest guild revision.');
        result=await this.one('UPDATE madlib_guilds SET finances_paused=$2,revision=revision+1,updated_at=now() WHERE guild_id=$1 RETURNING *',[guild,action==='pause'],c);
      }else if(['suspend','unsuspend'].includes(action)){
        const p=await this.one('SELECT * FROM madlib_publications WHERE id=$1 AND guild_id=$2 FOR UPDATE',[record,guild],c);check(p&&p.revision===revision,'STALE','Inspect the publication again and use its latest revision.');
        result=await this.one('UPDATE madlib_publications SET suspended=$2,moderation_reason=$3,revision=revision+1,display_dirty=TRUE WHERE id=$1 RETURNING *',[record,action==='suspend',evidence],c);
      }else{
        const o=await this.one('SELECT *,clock_timestamp() AS db_now FROM madlib_operations WHERE id=$1 AND guild_id=$2 FOR UPDATE',[record,guild],c);check(o&&o.revision===revision,'STALE','Inspect the operation again and use its latest revision.');
        if(['mark-sent','mark-not-sent'].includes(action)){
          check(o.state==='needs_review'&&(!o.sent_at||o.db_now-o.sent_at>=300000),'STATE','Only an uncertain operation at least five minutes after its send may be reconciled. Check authoritative provider evidence first.');
          if(action==='mark-sent')result=await this.finishOperation(o,evidence,c);
          else{result=await this.one("UPDATE madlib_operations SET state='confirmed_failure',error_code='ADMIN_VERIFIED_NOT_SENT',lease_id=NULL,revision=revision+1,updated_at=now() WHERE id=$1 RETURNING *",[record],c);if(o.kind==='debit')await c.query("UPDATE madlib_sessions SET state='payment_failed',revision=revision+1 WHERE id=$1 AND state='payment_review'",[o.session_id]);}
        }else if(action==='retry'){
          check(['confirmed_failure','retryable_failure'].includes(o.state),'STATE','Only a confirmed-not-sent operation can be retried. Unknown outcomes need evidence first.');
          if(o.kind==='debit'){const s=await this.one('SELECT state FROM madlib_sessions WHERE id=$1 FOR UPDATE',[o.session_id],c);check(s&&['payment_failed','awaiting_payment'].includes(s.state),'STATE','A cancelled or completed play cannot be charged.');await c.query("UPDATE madlib_sessions SET state='awaiting_payment',revision=revision+1 WHERE id=$1",[o.session_id]);}
          result=await this.one("UPDATE madlib_operations SET state='prepared',lease_id=NULL,lease_until=NULL,next_attempt_at=now(),revision=revision+1,updated_at=now() WHERE id=$1 RETURNING *",[record],c);
        }else if(action==='refund'){
          check(o.kind==='debit'&&o.state==='confirmed_success','STATE','Refund requires a confirmed play debit and a documented system failure.');
          const s=await this.one('SELECT * FROM madlib_sessions WHERE id=$1 FOR UPDATE',[o.session_id],c);check(s&&s.state==='active','STATE','Only an unfinished paid session prevented by a confirmed system failure can be refunded.');
          result=await this.one(`INSERT INTO madlib_operations(id,guild_id,user_id,kind,amount,realm_id,currency_id,session_id,sender_id,recipient_id)
            VALUES($1,$2,$3,'refund',$4,$5,$6,$7,$8,$9) ON CONFLICT(id) DO NOTHING RETURNING *`,[`madlib_refund:${s.id}`,guild,o.user_id,o.amount,o.realm_id,o.currency_id,s.id,o.recipient_id,o.sender_id],c);
          check(result,'STATE','A refund already exists. Inspect that operation instead.');await c.query("UPDATE madlib_sessions SET state='refund_pending',revision=revision+1 WHERE id=$1",[s.id]);
        }else check(false,'ACTION','Unknown administrative action.');
      }
      await this.audit(c,guild,actor,action,record,evidence,{previousRevision:revision,resultRevision:result.revision});return result;
    });
  }
  async status(guild){
    await this.query('INSERT INTO madlib_guilds(guild_id) VALUES($1) ON CONFLICT DO NOTHING',[guild]);
    return {guild:await this.one('SELECT * FROM madlib_guilds WHERE guild_id=$1',[guild]),
      operations:(await this.query('SELECT state,kind,COUNT(*)::integer AS count,SUM(amount)::bigint AS amount FROM madlib_operations WHERE guild_id=$1 GROUP BY state,kind',[guild])).rows,
      review:(await this.query("SELECT id,kind,state,revision,error_code FROM madlib_operations WHERE guild_id=$1 AND state IN ('needs_review','confirmed_failure','retryable_failure') ORDER BY updated_at LIMIT 20",[guild])).rows,
      publications:(await this.query("SELECT id,status,revision,moderation_reason FROM madlib_publications WHERE guild_id=$1 AND status='needs_review' ORDER BY updated_at LIMIT 20",[guild])).rows};
  }
  async claimReconcile(){
    return this.tx(async c=>{
      const p=await this.one(`SELECT * FROM madlib_publications WHERE status IN ('published','needs_review') AND next_reconcile_at<=now()
        AND (lease_until IS NULL OR lease_until<now()) ORDER BY next_reconcile_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1`,[],c);
      if(!p)return null;return this.one("UPDATE madlib_publications SET lease_id=$2,lease_until=now()+interval '2 minutes',next_reconcile_at=now()+interval '5 minutes' WHERE id=$1 RETURNING *",[p.id,id()],c);
    });
  }
  async releaseReconcile(p,{after=p.reconcile_after,type=p.reconcile_type,before=p.scan_before,delay=300,deleted=false}={}){
    return this.query(`UPDATE madlib_publications SET reconcile_after=$3,reconcile_type=$4,scan_before=$5,
      next_reconcile_at=now()+($6::integer*interval '1 second'),lease_id=NULL,lease_until=NULL,
      status=CASE WHEN $7 THEN 'deleted' ELSE status END,updated_at=now() WHERE id=$1 AND lease_id=$2`,[p.id,p.lease_id,after,type,before,delay,deleted]);
  }
}
module.exports={MadlibStore,ACTIVE};

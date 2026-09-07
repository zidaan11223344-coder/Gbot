import os, json, uuid, asyncio, logging, re
from pathlib import Path
from datetime import datetime, timezone
from supabase import create_client, Client

BASE=Path(__file__).resolve().parent
STATE=BASE/'control_state.json'
BOTS_FILE=BASE/'control_bots.json'
MASTERS_FILE=BASE/'room_masters.json'
LANG_FILE=BASE/'language.json'
LOG_DIR=BASE/'logs'; LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | control | %(message)s', handlers=[logging.FileHandler(LOG_DIR/'control.log',encoding='utf-8'), logging.StreamHandler()])
log=logging.getLogger('control')

SERVER_URL=os.environ.get('SUPABASE_URL','').strip().rstrip('/')
# SUPABASE_PUBLISHABLE_KEY is preferred for new Supabase projects.
# SUPABASE_KEY remains supported for legacy deployments.
SERVER_KEY=(os.environ.get('SUPABASE_PUBLISHABLE_KEY') or os.environ.get('SUPABASE_ANON_KEY') or os.environ.get('SUPABASE_KEY') or '').strip()
CONTROL_USERNAME=os.environ.get('GIANT_USERNAME','').strip()
CONTROL_PASSWORD=os.environ.get('GIANT_PASSWORD','')
DEFAULT_LANG=(os.environ.get('CONTROL_LANGUAGE') or 'ar').strip().lower()
ROOM_PASSWORD=os.environ.get('ROOM_PASSWORD','')
POLL=float(os.environ.get('CONTROL_POLL_SECONDS','2'))
CONTROL_BOT_VERSION='username-login-v4-userid'
if not SERVER_URL or not SERVER_KEY or not CONTROL_USERNAME or not CONTROL_PASSWORD:
    raise SystemExit('Missing SUPABASE_URL/SUPABASE_KEY/GIANT_USERNAME/GIANT_PASSWORD')

def create_supabase_client(url, key):
    """إنشاء عميل Supabase بنفس طريقة bot.py العامل.

    بعض إصدارات supabase-py تتحقق محلياً من أن المفتاح JWT، بينما
    sb_publishable_* ليس JWT. لذلك نستخدم JWT شكلياً أثناء الإنشاء، ثم
    نستبدل رأس API بالمفتاح الحقيقي ونحذف Authorization الخاص بالمفتاح.
    """
    if str(key).startswith('sb_publishable_'):
        client = create_client(url, 'a.b.c')
        client.supabase_key = key
        headers = client.options.headers
        headers['apiKey'] = key
        headers.pop('Authorization', None)
        return client
    return create_client(url, key)

sb: Client=create_supabase_client(SERVER_URL, SERVER_KEY)
BOT_ID=None
last_dm=datetime.now(timezone.utc).isoformat()
last_room={}
child_tasks={}
child_clients={}

def load(path, default):
    try:
        if path.exists():
            x=json.loads(path.read_text(encoding='utf-8')); return x
    except Exception: log.exception('state load failed %s',path)
    return default

def save(path, obj):
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')

def language_state():
    data=load(LANG_FILE,{})
    if not isinstance(data,dict): data={}
    # Keep compatibility with the old global language format.
    data.setdefault('default', data.get('lang', DEFAULT_LANG))
    data.setdefault('rooms', {})
    data.setdefault('users', {})
    data.setdefault('pending', {})
    return data

def lang(room_id=None, user_id=None):
    data=language_state()
    if room_id is not None and str(room_id) in data.get('rooms',{}):
        return str(data['rooms'][str(room_id)]).lower()
    if user_id is not None and str(user_id) in data.get('users',{}):
        return str(data['users'][str(user_id)]).lower()
    return str(data.get('default',DEFAULT_LANG)).lower()

def L(ar,en,room_id=None,user_id=None):
    return ar if lang(room_id,user_id)!='en' else en

def set_user_lang(user_id,value):
    data=language_state(); data['users'][str(user_id)]=value; save(LANG_FILE,data)

def set_room_lang(room_id,value):
    data=language_state(); data['rooms'][str(room_id)]=value; save(LANG_FILE,data)

def set_pending_language(user_id,room_id,room_name):
    data=language_state(); data['pending'][str(user_id)]={'room_id':str(room_id),'room_name':room_name,'created_at':now()}; save(LANG_FILE,data)

def pending_language(user_id):
    return language_state().get('pending',{}).get(str(user_id))

def pop_pending_language(user_id):
    data=language_state(); item=data.get('pending',{}).pop(str(user_id),None); save(LANG_FILE,data); return item

def now(): return datetime.now(timezone.utc).isoformat()
async def run(fn):
    try: return await asyncio.to_thread(fn)
    except Exception as e: log.warning('db: %s',e); return None
async def select(table, cols='*', **filters):
    def f():
        q=sb.table(table).select(cols)
        for k,v in filters.items(): q=q.eq(k,v)
        return q.execute().data or []
    return await run(f) or []
async def rpc(name,args):
    return await run(lambda: sb.rpc(name,args).execute().data)

async def resolve_email(client, username):
    """Resolve Giant Chat username using the same order as the working bot."""
    username = str(username or '').strip()
    if not username:
        raise RuntimeError('Giant username is empty')

    # Giant Chat's app login accepts username/password, but Supabase Auth
    # receives the deterministic internal email used by the app.
    # Do not require the user to know or provide that email.
    normalized = re.sub(r'[^a-z0-9_]', '', username.lower())
    if not normalized:
        raise RuntimeError('Unable to resolve Giant username')
    email = f'{normalized}@giant.app'
    log.info('Using Giant username mapping for authentication: %s -> internal email', username)
    return email

async def login_client(username,password):
    client=create_supabase_client(SERVER_URL,SERVER_KEY)
    email=await resolve_email(client,username)
    res=await run(lambda: client.auth.sign_in_with_password({'email':email,'password':password}))
    user=getattr(res,'user',None) if res else None
    user_id=getattr(user,'id',None)
    if not user_id: return None, 'login succeeded but Supabase returned no user id'
    # Recent gotrue clients do not always populate client.auth.user after sign-in.
    # Keep the authoritative id returned by sign_in_with_password for room checks.
    client._giant_user_id=str(user_id)
    return client, None

async def find_room(client,name):
    rows=await asyncio.to_thread(lambda: client.table('rooms').select('id,name').eq('name',name.strip()).limit(1).execute().data or [])
    return rows[0] if rows else None

async def join_bot(client, room):
    return await run(lambda: client.rpc('room_join',{'_room':room['id'],'_password':ROOM_PASSWORD}).execute())
async def leave_bot(client, room_id):
    return await run(lambda: client.rpc('room_leave',{'_room':room_id}).execute())
async def heartbeat_bot(client, room_id):
    return await run(lambda: client.rpc('room_heartbeat',{'_room':room_id}).execute())

# A feature/control bot is accepted into a room only when Giant reports
# that the bot itself has moderator/admin privileges. A normal member is
# never registered as an active managed bot.
BOT_ALLOWED_RANKS = {'moderator', 'admin'}

async def member_rank(client, room_id, user_id):
    try:
        rows = await asyncio.to_thread(lambda: client.table('room_members').select('rank').eq('room_id', room_id).eq('user_id', str(user_id)).limit(1).execute().data or [])
        if not rows:
            return None
        return str(rows[0].get('rank') or 'member').strip().lower()
    except Exception as exc:
        log.warning('rank lookup failed room=%s user=%s: %s', room_id, user_id, exc)
        return None

def client_user_id(client):
    uid=getattr(client,'_giant_user_id',None)
    if uid:
        return str(uid)
    user=getattr(getattr(client,'auth',None),'user',None)
    return str(getattr(user,'id',None) or '') or None

async def require_bot_admin(client, room):
    uid = client_user_id(client)
    if not uid:
        return False, 'bot user id unavailable'
    rank = await member_rank(client, room['id'], uid)
    if rank not in BOT_ALLOWED_RANKS:
        return False, rank or 'not_member'
    return True, rank

async def announce(client, room_id, text):
    # Used only for connection status; feature bots remain independent.
    try:
        uid=client_user_id(client)
        if uid:
            await asyncio.to_thread(lambda: client.table('room_messages').insert({'room_id':room_id,'user_id':uid,'content':text,'message_type':'text'}).execute())
    except Exception: pass


# ============================================================================
# [قسم تحكم الغرف المنقول من bot.py] BEGIN
# ============================================================================

ROOM_CONTROL_STATE = BASE / "room_control_state.json"
ROOM_REPLIES_STATE = BASE / "room_replies.json"
ROOM_WELCOME_STATE = BASE / "room_welcome.json"

def norm(s):
    v=str(s or "").strip().lower()
    v=re.sub(r"[\u064b-\u065f\u0670\u0640]", "", v)
    v=v.replace("ـ","")
    return re.sub(r"\s+", " ", v)

def _room_state():
    x=load(ROOM_CONTROL_STATE,{})
    return x if isinstance(x,dict) else {}

def _save_room_state(x): save(ROOM_CONTROL_STATE,x)

def _room_replies():
    x=load(ROOM_REPLIES_STATE,{})
    return x if isinstance(x,dict) else {}

def _save_room_replies(x): save(ROOM_REPLIES_STATE,x)

def _room_welcome():
    x=load(ROOM_WELCOME_STATE,{})
    return x if isinstance(x,dict) else {}

def _save_room_welcome(x): save(ROOM_WELCOME_STATE,x)

async def room_username(client, uid):
    try:
        rows=await asyncio.to_thread(lambda: client.table('profiles').select('username').eq('id',str(uid)).limit(1).execute().data or [])
        return str(rows[0].get('username') or uid) if rows else str(uid)
    except Exception:
        return str(uid)

async def child_profile(client, username):
    clean=str(username or "").strip().lstrip("@")
    if not clean: return None
    try:
        rows=await asyncio.to_thread(lambda: client.table('profiles').select('id,username').eq('username',clean).limit(1).execute().data or [])
        return rows[0] if rows else None
    except Exception:
        return None

async def child_rpc(client, name, args):
    try:
        return await asyncio.to_thread(lambda: client.rpc(name,args).execute().data)
    except Exception as exc:
        log.warning("room control rpc %s failed: %s", name, exc)
        return None

async def child_send_room(client, room_id, text):
    if not text: return False
    uid=client_user_id(client)
    if not uid: return False
    envelope={'v':1,'id':str(uuid.uuid4()),'content':str(text),'message_type':'text',
              'media_url':None,'media_duration_ms':None,'reply_to_id':None,'created_at':now()}
    try:
        await asyncio.to_thread(lambda: client.table('room_messages').insert({
            'room_id':room_id,'user_id':uid,'content':str(text),'message_type':'text'
        }).execute())
        return True
    except Exception:
        return False

async def child_is_room_master(client, room_id, uid):
    # Room master is the owner/delegate stored by the control bot.
    rec=load(MASTERS_FILE,{}).get(str(room_id))
    if not rec: return False
    sid=str(uid)
    return sid == str(rec.get('master_id','')) or sid in [str(x) for x in (rec.get('masters') or [])]

async def child_target_id(client, username):
    p=await child_profile(client,username)
    return (str(p.get('id')), p.get('username') or username) if p else (None,None)

async def child_moderate(client, room_id, action, target_username, minutes=0):
    tid,tname=await child_target_id(client,target_username)
    if not tid:
        return False, f"❌ المستخدم @{str(target_username).lstrip('@')} غير موجود."
    if tid == client_user_id(client):
        return False, "❌ لا يمكن للبوت تنفيذ الإجراء على نفسه."
    if action == 'kick':
        data=await child_rpc(client,'kick_room_member',{'_room':room_id,'_user':tid})
    elif action == 'ban':
        data=await child_rpc(client,'ban_room_member',{'_room':room_id,'_user':tid,'_reason':'إجراء إداري عبر بوت التحكم'})
    elif action == 'mute':
        # Use temporary local mute + server-side rank/member action when available.
        data=await child_rpc(client,'mute_room_member',{'_room':room_id,'_user':tid,'_minutes':int(minutes or 5)})
    elif action == 'rank':
        data=await child_rpc(client,'set_member_rank',{'_room':room_id,'_user':tid,'_new_rank':'moderator'})
    else:
        return False, "❌ إجراء غير مدعوم."
    if data is None:
        return False, "❌ رفض Giant الإجراء أو لم تُرجع قاعدة البيانات نتيجة."
    return True, tname

def child_filter_words(room_id):
    st=_room_state()
    item=st.setdefault(str(room_id),{'filter':False,'words':[],'muted':{},'pending':{}})
    return item

async def child_handle_admin(client, rec, room_id, sender_id, text, from_dm=False):
    t=str(text or "").strip()
    low=norm(t)
    if not t: return None
    sender_name=await room_username(client,sender_id)

    # In-room commands may be used by the room master/delegates.
    # In DM, only the room master/delegates linked to this room may control it.
    if not await child_is_room_master(client, room_id, sender_id):
        if from_dm:
            return "🚫 هذا البوت يقبل أوامر هذه الغرفة من الماستر المعيّن فقط."
        return None

    state=child_filter_words(room_id)
    replies=_room_replies()
    welcome=_room_welcome()

    # Help
    if low in ('help','مساعدة','الاوامر','الأوامر'):
        return L(
            "🛡️ أوامر التحكم:\n"
            "حظر → ثم اسم المستخدم\n"
            "طرد → ثم اسم المستخدم\n"
            "كتم → ثم اسم المستخدم ثم المدة بالدقائق\n"
            "فك الكتم → ثم اسم المستخدم\n"
            "+mf@كلمة | -mf@كلمة\n"
            "mf@on / mf@off / l@mf / clear@mf\n"
            "+r@كلمة@الرد\n"
            "lr — عرض الردود\n"
            "cr@كلمة — حذف رد\n"
            "+wc نص الترحيب\n"
            "wc@on / wc@off / l@wc / clear@wc\n"
            "صلاحياتي\n"
            "حالة البوت",
            "🛡️ Control commands:\n"
            "حظر / طرد / كتم / فك الكتم\n"
            "+mf@word / -mf@word\n"
            "mf@on / mf@off / l@mf / clear@mf\n"
            "+r@word@reply / lr / cr@word\n"
            "+wc welcome / wc@on / wc@off / l@wc / clear@wc\n"
            "صلاحياتي / حالة البوت"
        )

    # Interactive moderation like bot.py.
    pending=state.setdefault('pending',{})
    if low in ('حظر','طرد','كتم','فك الكتم','فك_الكتم','unmute'):
        pending[str(sender_id)]={'action':('ban' if low=='حظر' else 'kick' if low=='طرد' else 'mute' if low=='كتم' else 'unmute'),'created_at':time.time()}
        return f"✍️ أرسل اسم المستخدم لتنفيذ «{t}»."

    if str(sender_id) in pending and time.time()-float(pending[str(sender_id)].get('created_at',0)) <= 120 and low not in ('الغاء','إلغاء','cancel'):
        p=pending[str(sender_id)]
        # only treat non-command text as the target for interactive mode
        if '@' not in t and not low.startswith(('+mf','-mf','mf@','+r@','+wc','wc@','l@','clear@','صلاحيات','حالة')):
            target=t.lstrip('@').split()[0]
            pending.pop(str(sender_id),None)
            if p['action']=='mute':
                parts=t.split()
                target=parts[0].lstrip('@')
                minutes=int(parts[1]) if len(parts)>1 and parts[1].isdigit() else 5
                ok,msg=await child_moderate(client,room_id,'mute',target,minutes)
            elif p['action']=='unmute':
                tid,tname=await child_target_id(client,target)
                ok,msg=(False,"المستخدم غير موجود.") if not tid else (True,tname)
                if ok:
                    rs=child_filter_words(room_id); rs.setdefault('muted',{}).pop(str(tid),None); _save_room_state(_room_state())
                    data=await child_rpc(client,'unmute_room_member',{'_room':room_id,'_user':tid})
                    if data is None: ok=False; msg="رفض Giant فك الكتم."
            else:
                ok,msg=await child_moderate(client,room_id,p['action'],target)
            return (f"✅ تم تنفيذ الأمر على @{msg}." if ok else str(msg))

    if low.startswith('حظر@') or low.startswith('ban@'):
        target=t.split('@',1)[1].strip()
        ok,msg=await child_moderate(client,room_id,'ban',target)
        return f"✅ تم حظر @{msg}." if ok else msg
    if low.startswith('طرد@') or low.startswith('kick@'):
        target=t.split('@',1)[1].strip()
        ok,msg=await child_moderate(client,room_id,'kick',target)
        return f"✅ تم طرد @{msg}." if ok else msg
    if low.startswith('كتم@') or low.startswith('mute@'):
        parts=t.split('@',2); target=parts[1].strip() if len(parts)>1 else ""
        minutes=int(parts[2]) if len(parts)>2 and parts[2].strip().isdigit() else 5
        ok,msg=await child_moderate(client,room_id,'mute',target,minutes)
        return f"✅ تم كتم @{msg} لمدة {minutes} دقيقة." if ok else msg
    if low.startswith('فك@') or low.startswith('فك الكتم@') or low.startswith('unmute@'):
        target=t.split('@',1)[1].strip()
        tid,tname=await child_target_id(client,target)
        if not tid: return "❌ المستخدم غير موجود."
        rs=child_filter_words(room_id); rs.setdefault('muted',{}).pop(str(tid),None); _save_room_state(_room_state())
        data=await child_rpc(client,'unmute_room_member',{'_room':room_id,'_user':tid})
        return f"✅ تم فك الكتم عن @{tname}." if data is not None else "❌ تعذر فك الكتم."

    # Word filter, exactly matching bot.py style.
    if low in ('mf@on','mf on'):
        state['filter']=True; _save_room_state(_room_state()); return "✅ تم تفعيل فلتر الألفاظ."
    if low in ('mf@off','mf off'):
        state['filter']=False; _save_room_state(_room_state()); return "⛔ تم تعطيل فلتر الألفاظ."
    if low=='clear@mf':
        state['words']=[]; _save_room_state(_room_state()); return "🧹 تم حذف جميع الكلمات الممنوعة."
    if low=='l@mf':
        return "🚫 الكلمات الممنوعة:\n"+("\n".join(f"{i+1}. {w}" for i,w in enumerate(state.get('words',[]))) if state.get('words') else "لا توجد كلمات.")
    if low.startswith('+mf@'):
        w=t.split('@',1)[1].strip()
        if not w: return "❌ الصيغة: +mf@كلمة"
        if w not in state.setdefault('words',[]): state['words'].append(w)
        _save_room_state(_room_state()); return f"✅ تمت إضافة الكلمة الممنوعة: {w}"
    if low.startswith('-mf@'):
        w=t.split('@',1)[1].strip()
        state['words']=[x for x in state.get('words',[]) if norm(x)!=norm(w)]
        _save_room_state(_room_state()); return f"✅ تمت إزالة الكلمة: {w}"

    # Custom replies.
    room_rep=replies.setdefault(str(room_id),{})
    if low.startswith('+r@'):
        parts=t.split('@',2)
        if len(parts)<3: return "❌ الصيغة: +r@الكلمة@الرد"
        room_rep[parts[1].strip()]=parts[2].strip(); _save_room_replies(replies)
        return f"✅ تم إضافة الرد للكلمة: {parts[1].strip()}"
    if low=='lr':
        return "💬 الردود:\n"+("\n".join(f"• {k} → {v}" for k,v in room_rep.items()) if room_rep else "لا توجد ردود.")
    if low.startswith('cr@'):
        k=t.split('@',1)[1].strip(); room_rep.pop(k,None); _save_room_replies(replies)
        return f"✅ تم حذف الرد: {k}"

    # Welcome.
    if low.startswith('+wc '):
        msg=t.split(' ',1)[1].strip()
        item=welcome.setdefault(str(room_id),{'enabled':False,'messages':[]})
        if msg not in item['messages']: item['messages'].append(msg)
        _save_room_welcome(welcome); return "✅ تمت إضافة رسالة الترحيب."
    if low=='clear@wc':
        welcome.pop(str(room_id),None); _save_room_welcome(welcome); return "🧹 تم حذف رسائل الترحيب."
    if low=='l@wc':
        msgs=welcome.get(str(room_id),{}).get('messages',[])
        return "👋 رسائل الترحيب:\n"+("\n".join(f"{i+1}. {m}" for i,m in enumerate(msgs)) if msgs else "لا توجد رسائل.")
    if low in ('wc@on','wc on'):
        welcome.setdefault(str(room_id),{'enabled':False,'messages':[]})['enabled']=True; _save_room_welcome(welcome); return "✅ تم تفعيل رسائل الترحيب."
    if low in ('wc@off','wc off'):
        welcome.setdefault(str(room_id),{'enabled':False,'messages':[]})['enabled']=False; _save_room_welcome(welcome); return "⛔ تم تعطيل رسائل الترحيب."

    if low in ('صلاحياتي','modstatus','حالة البوت'):
        rank=await member_rank(client,room_id,client_user_id(client)) or 'unknown'
        return f"🤖 رتبة البوت: {rank}\n👤 @{sender_name} هو ماستر الغرفة: نعم"

    # Normal configured reply.
    if t in room_rep:
        return room_rep[t]

    # Filter incoming normal messages before they fall through.
    if state.get('filter'):
        nt=norm(t); compact=nt.replace(' ','')
        for w in state.get('words',[]):
            nw=norm(w); cw=nw.replace(' ','')
            if nw and (nw in nt or (cw and cw in compact)):
                tid,tname=await child_target_id(client,sender_name)
                if tid and str(sender_id)!=str(tid):
                    ok,msg=await child_moderate(client,room_id,'ban',sender_name)
                    return "🚫 تم حظر الحساب بسبب كلمة محظورة." if ok else f"⚠️ تم اكتشاف الكلمة لكن تعذر الحظر: {msg}"

    return None

async def child_room_loop(rec):
    client=child_clients.get(rec['id'])
    room_id=rec.get('room_id')
    if not client or not room_id: return
    cursor=now()
    while True:
        try:
            rows=await asyncio.to_thread(
                lambda: client.table('room_messages').select('*')
                .eq('room_id',room_id).gt('created_at',cursor)
                .order('created_at').limit(50).execute().data or []
            )
            for m in rows:
                cursor=m.get('created_at') or cursor
                uid=m.get('user_id')
                if not uid or str(uid)==str(client_user_id(client)) or m.get('message_type')=='system':
                    continue
                text=str(m.get('content') or '').strip()
                reply=await child_handle_admin(client,rec,room_id,str(uid),text,from_dm=False)
                if reply:
                    await child_send_room(client,room_id,reply)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('child room loop failed for %s',rec.get('username'))
        await asyncio.sleep(max(1.0,POLL))

async def child_dm_loop(rec):
    client=child_clients.get(rec['id'])
    room_id=rec.get('room_id')
    if not client: return
    last=now()
    child_id=client_user_id(client)
    while True:
        try:
            rows=await asyncio.to_thread(
                lambda: client.table('dm_relay').select('*')
                .eq('recipient_id',str(child_id)).gt('created_at',last)
                .order('created_at').limit(50).execute().data or []
            )
            for row in rows:
                last=row.get('created_at') or last
                sender=row.get('sender_id'); env=row.get('envelope') or {}
                text=str(env.get('content') or '').strip()
                if not sender or not text or str(sender)==str(child_id): continue
                reply=await child_handle_admin(client,rec,room_id,str(sender),text,from_dm=True)
                if reply:
                    await asyncio.to_thread(lambda s=sender,e={
                        'v':1,'id':str(uuid.uuid4()),'content':reply,'message_type':'text',
                        'media_url':None,'media_duration_ms':None,'reply_to_id':None,'created_at':now()
                    }: client.table('dm_relay').insert({'sender_id':child_id,'recipient_id':str(s),'envelope':e}).execute())
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('child dm loop failed for %s',rec.get('username'))
        await asyncio.sleep(max(1.0,POLL))

# ============================================================================
# [قسم تحكم الغرف المنقول من bot.py] END
# ============================================================================

async def child_runner(rec):
    bid=rec['id']; username=rec['username']; password=rec['password']
    role=rec.get('role','main'); room_id=rec.get('room_id'); room_name=rec.get('room_name','')
    client,err=await login_client(username,password)
    if err:
        rec['status']='error'; rec['error']=err; save(BOTS_FILE, load(BOTS_FILE,[])); return
    child_clients[bid]=client
    rec['status']='online'; rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))
    try:
        room={'id':room_id,'name':room_name}
        joined=await join_bot(client,room)
        if joined is None:
            rec['status']='error'; rec['error']='room_join failed'
            save(BOTS_FILE,load(BOTS_FILE,[])); return

        rank = await member_rank(client, room_id, client_user_id(client))
        rec['rank']=rank or 'unknown'
        # Only the Main control bot must have Moderator/Admin rights.
        # Hang/silent bots are allowed regardless of their current rank.
        if role == 'main':
            ready, rank = await require_bot_admin(client, room)
            if not ready:
                rec['status']='error'
                rec['error']=f'bot rank is {rank}; moderator/admin required'
                save(BOTS_FILE,load(BOTS_FILE,[]))
                try: await leave_bot(client,room_id)
                except Exception: pass
                return

        rec['rank']=rank or rec.get('rank') or 'unknown'
        rec['status']='online'; rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))

        room_task = None
        dm_task = None
        # Main bot is the only room controller. Hang bots stay silent.
        if role == 'main':
            room_task = asyncio.create_task(child_room_loop(rec), name=f'room-{username}')
            dm_task = asyncio.create_task(child_dm_loop(rec), name=f'dm-{username}')
        try:
            while True:
                # Main controller must remain Moderator/Admin. Silent bots keep running
                # even as normal member/visitor.
                if role == 'main':
                    current_rank = await member_rank(client, room_id, client_user_id(client))
                    if current_rank not in BOT_ALLOWED_RANKS:
                        rec['status']='error'
                        rec['error']=f'bot rank changed to {current_rank or "unknown"}; moderator/admin required'
                        save(BOTS_FILE,load(BOTS_FILE,[]))
                        break
                    rec['rank']=current_rank
                else:
                    rec['rank'] = await member_rank(client, room_id, client_user_id(client)) or rec.get('rank') or 'unknown'
                await heartbeat_bot(client,room_id)
                await asyncio.sleep(15)
        finally:
            for task in (room_task, dm_task):
                if task:
                    task.cancel()
            for task in (room_task, dm_task):
                if task:
                    try: await task
                    except asyncio.CancelledError: pass
    except asyncio.CancelledError:
        pass
    except Exception as e:
        rec['status']='error'; rec['error']=str(e)[:240]
    finally:
        try: await leave_bot(client,room_id)
        except Exception: pass
        child_clients.pop(bid,None)
        if rec.get('status') != 'error':
            rec['status']='offline'
        rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))

async def send_dm(user_id,text):
    envelope={'v':1,'id':str(uuid.uuid4()),'content':text,'message_type':'text','media_url':None,'media_duration_ms':None,'reply_to_id':None,'created_at':now()}
    try:
        await asyncio.to_thread(lambda: sb.table('dm_relay').insert({'sender_id':BOT_ID,'recipient_id':str(user_id),'envelope':envelope}).execute())
        return True
    except Exception as exc:
        log.warning('send_dm failed to %s: %s',user_id,exc)
        return False

def language_prompt(room_name):
    return (f'🌐 تم قبول البوت في غرفة {room_name} بنجاح.\n\n'
            'اختر لغة بوت التحكم بإرسال:\n'
            '1️⃣ العربية\n'
            '2️⃣ English\n\n'
            'بعد اختيار اللغة أرسل help لعرض جميع الخيارات.')

async def add_bot(username,password,room_name,role='main',master_id='',master_name=''):
    bots=load(BOTS_FILE,[])
    masters=load(MASTERS_FILE,{})
    room=await find_room(sb,room_name)
    if not room: return L('❌ الغرفة غير موجودة.','❌ Room not found.')
    room_key=str(room['id'])
    current=masters.get(room_key)
    is_first_bot = not bool(current)
    if current:
        sid=str(master_id)
        allowed=[str(current.get('master_id',''))] + [str(x) for x in (current.get('masters') or [])]
        if sid not in allowed:
            return L(f'🚫 هذه الغرفة مرتبطة بالماستر @{current.get("master_name","")} والماسترات المضافين فقط.', f'🚫 This room is controlled by @{current.get("master_name","")} and its added masters only.')
        if role == 'main' and any(str(b.get('room_id')) == room_key and b.get('role','main') == 'main' for b in bots):
            return L('⚠️ يوجد بالفعل بوت تحكم واحد لهذه الغرفة.', '⚠️ This room already has one Main control bot.')
    # Verify credentials immediately: automatic acceptance only after successful login.
    client,err=await login_client(username,password)
    if err: return L('❌ بيانات البوت غير صحيحة أو تعذر تسجيل الدخول.','❌ Bot credentials are invalid or login failed.')
    try: await join_bot(client,room)
    except Exception: return L('❌ تعذر إدخال البوت إلى الغرفة.','❌ Could not join the room.')
    ready, rank = await require_bot_admin(client, room)
    if not ready:
        try: await leave_bot(client, room['id'])
        except Exception: pass
        return L(
            f'❌ لم تتم إضافة @{username}. يجب أن تكون رتبة البوت داخل الغرفة «مشرف» أو «ادمن» أولاً. الرتبة الحالية: «{rank or "غير معروف"}».',
            f'❌ @{username} was not added. The bot must be Moderator or Admin first. Current rank: {rank or "unknown"}.'
        )
    bid=str(uuid.uuid4())
    rec={'id':bid,'username':username,'password':password,'room_id':room['id'],'room_name':room['name'],'role':role,'rank':rank,'master_id':str(master_id),'master_name':master_name,'status':'online','created_at':now(),'updated_at':now()}
    bots.append(rec); save(BOTS_FILE,bots)
    if not current:
        masters[room_key]={'master_id':str(master_id),'master_name':master_name,'masters':[],'master_names':[],'room_name':room['name'],'created_at':now(),'updated_at':now()}
    else:
        current['room_name']=room['name']; current['updated_at']=now()
        masters[room_key]=current
    save(MASTERS_FILE,masters)
    # close temporary client; persistent child reconnects independently
    try: await asyncio.to_thread(lambda: client.auth.sign_out())
    except Exception: pass
    task=asyncio.create_task(child_runner(rec),name=f'bot-{username}')
    child_tasks[bid]=task
    # Send the language chooser whenever this master has no saved choice yet.
    # This also repairs rooms whose old masters.json predates language.json.
    lang_state=language_state()
    has_language=(str(room['id']) in lang_state.get('rooms',{})) or (str(master_id) in lang_state.get('users',{}))
    prompt_sent=False
    if not has_language:
        set_pending_language(master_id, room['id'], room['name'])
        prompt_sent=await send_dm(master_id, language_prompt(room['name']))
        if not prompt_sent:
            log.error('Language prompt could not be delivered to master %s for room %s', master_id, room['name'])
    language_note='\n📩 تم إرسال اختيار اللغة إلى خاصك.' if prompt_sent else ''
    language_note_en='\n📩 Language selection was sent to your DM.' if prompt_sent else ''
    return L(f'✅ تمت إضافة @{username} تلقائياً إلى غرفة {room["name"]}.\n🤖 النوع: {"Main Bot" if role=="main" else "Hang Bot"}'+language_note,f'✅ @{username} was automatically added to {room["name"]}.\n🤖 Type: {"Main Bot" if role=="main" else "Hang Bot"}'+language_note_en)

async def remove_bot(rec):
    task=child_tasks.pop(rec['id'],None)
    if task: task.cancel()
    client=child_clients.get(rec['id'])
    if client:
        try: await leave_bot(client,rec['room_id'])
        except Exception: pass

async def resolve_profile(username):
    clean=str(username or '').strip().lstrip('@')
    if not clean:
        return None, 'empty username'
    try:
        rows=await asyncio.to_thread(lambda: sb.table('profiles').select('id,username').eq('username',clean).limit(1).execute().data or [])
        if not rows: return None, 'not found'
        return rows[0], None
    except Exception as exc:
        log.warning('profile lookup failed: %s',exc)
        return None, str(exc)

async def username_of(user_id):
    try:
        rows=await asyncio.to_thread(lambda: sb.table('profiles').select('username').eq('id',str(user_id)).limit(1).execute().data or [])
        return str(rows[0].get('username') or '').strip() if rows else ''
    except Exception:
        return ''

async def authorized_for_room(sender_id, room_name):
    room=await find_room(sb,room_name)
    if not room:
        return False, None, L('❌ الغرفة غير موجودة.','❌ Room not found.')
    masters=load(MASTERS_FILE,{})
    rec=masters.get(str(room['id']))
    if not rec:
        return False, room, L('🚫 لا يوجد ماستر مسجل لهذه الغرفة بعد. أرسل أول إضافة عبر هذا البوت ليتم تعيينك ماستر تلقائياً.','🚫 No master is registered for this room yet. Send the first add command through this bot to become the master automatically.')
    sid=str(sender_id)
    primary=str(rec.get('master_id',''))
    delegates=[str(x) for x in (rec.get('masters') or [])]
    if sid not in ([primary] + delegates):
        name=rec.get('master_name','')
        return False, room, L(f'🚫 هذه الغرفة تحت تحكم @{name} والماسترات المضافين فقط.',f'🚫 This room is controlled only by @{name} and its added masters.')
    return True, room, ''

async def primary_master_for_room(sender_id, room_name):
    room=await find_room(sb,room_name)
    if not room:
        return False, None, L('❌ الغرفة غير موجودة.','❌ Room not found.')
    rec=load(MASTERS_FILE,{}).get(str(room['id']))
    if not rec:
        return False, room, L('🚫 لا يوجد ماستر لهذه الغرفة.','🚫 No master is registered for this room.')
    if str(rec.get('master_id','')) != str(sender_id):
        return False, room, L('🚫 هذا الأمر للماستر الأساسي للغرفة فقط.','🚫 This command is for the primary room master only.')
    return True, room, ''

async def control_action(sender,text):
    t=text.strip(); low=t.lower()
    if low in ('help','مساعدة','الاوامر','الأوامر'):
        return L('''al-sfeer:\nabot Bot Commands\n1. user@pass@room - Create Main bot\n2. hb@user@pass@room - Create Hang bot\n3. del@room - Delete main bots from a room\n4. delall@room - Delete all bots from a room\n5. clean@room - Clean room memory\n6. bots - Display active bots\n7. room@name - Display room information\n8. lang@ar / lang@en - Change language\n9. master@user@room - Add a master/delegate (primary master only)\n10. delmaster@user@room - Remove a delegate (primary master only)\n11. masters@room - List room masters\n\n⚠️ أول شخص يضيف بوتاً إلى الغرفة يصبح الماستر الأساسي بعد نجاح التحقق من رتبة البوت.''','''al-sfeer:\nabot Bot Commands\n1. user@pass@room - Create Main bot\n2. hb@user@pass@room - Create Hang bot\n3. del@room - Delete main bots from a room\n4. delall@room - Delete all bots from a room\n5. clean@room - Clean room memory\n6. bots - Display active bots\n7. room@name - Display room information\n8. lang@ar / lang@en - Change language\n9. master@user@room - Add a master/delegate (primary master only)\n10. delmaster@user@room - Remove a delegate (primary master only)\n11. masters@room - List room masters\n\n⚠️ The first user who successfully adds a bot becomes the primary master, only after the bot is Moderator/Admin.''', user_id=sender)
    # The first DM after a successful room creation is the language chooser.
    pending=pending_language(sender)
    if pending:
        choice=low.strip()
        if choice in ('1','ar','arabic','عربي','العربية'):
            value='ar'
        elif choice in ('2','en','english','انجليزي','الانجليزية','الإنجليزية'):
            value='en'
        else:
            return '🌐 اختر اللغة أولاً بإرسال:\n1️⃣ العربية\n2️⃣ English\n\nثم أرسل help لعرض الخيارات.'
        set_room_lang(pending.get('room_id'),value)
        set_user_lang(sender,value)
        pop_pending_language(sender)
        return ('✅ تم اختيار العربية لبوت التحكم في هذه الغرفة.\n📌 أرسل help لعرض جميع الخيارات.' if value=='ar'
                else '✅ English selected for the control bot in this room.\n📌 Send help to view all options.')

    if low.startswith('lang@') or low.startswith('لغة@'):
        masters=load(MASTERS_FILE,{})
        room_ids=[]
        for rid,x in masters.items():
            if str(x.get('master_id',''))==str(sender) or str(sender) in [str(v) for v in (x.get('masters') or [])]:
                room_ids.append(rid)
        if not room_ids:
            return '🚫 لا يمكنك تغيير اللغة. يجب أن تكون ماستر لغرفة.'
        pieces=t.split('@')
        v=pieces[1].strip().lower() if len(pieces)>=2 else ''
        v='ar' if v in ('ar','arabic','عربي','العربية') else ('en' if v in ('en','english','انجليزي','الانجليزية','الإنجليزية') else '')
        if not v: return 'استخدم: lang@ar أو lang@en / Use: lang@ar or lang@en'
        target_room_id=room_ids[0]
        if len(pieces)>=3:
            target_name='@'.join(pieces[2:]).strip()
            room_obj=await find_room(sb,target_name)
            if not room_obj or str(room_obj['id']) not in room_ids:
                return '🚫 لا يمكنك تغيير لغة هذه الغرفة.'
            target_room_id=str(room_obj['id'])
        elif len(room_ids)>1:
            return 'استخدم lang@ar@الغرفة أو lang@en@الغرفة لتحديد الغرفة. / Use lang@ar@room or lang@en@room.'
        set_room_lang(target_room_id,v); set_user_lang(sender,v)
        return 'تم تغيير لغة بوت التحكم إلى العربية.' if v=='ar' else 'Control bot language changed to English.'

    sender_name=(await username_of(sender)).strip().lstrip('@')
    # Format: username@password@room. Parse from the right so password may
    # contain @, e.g. username@gag@998877@Room Name.
    excluded=('room@','del@','delall@','clean@','lang@','لغة@','hb@','master@','delmaster@','masters@')
    if '@' in t and not low.startswith(excluded):
        payload, room = t.rsplit('@', 1)
        if '@' in payload:
            username, password = payload.split('@', 1)
            username=username.strip().lstrip('@'); password=password.strip(); room=room.strip()
            if username and password and room:
                return await add_bot(username,password,room,'main',str(sender),sender_name)
    if low.startswith('hb@'):
        payload=t[3:]
        if '@' in payload:
            payload, room = payload.rsplit('@', 1)
            if '@' in payload:
                username, password = payload.split('@', 1)
                if username.strip() and password.strip() and room.strip():
                    return await add_bot(username.strip().lstrip('@'),password.strip(),room.strip(),'hang',str(sender),sender_name)
    if low.startswith('delall@'):
        room=t.split('@',1)[1].strip(); ok,room_obj,msg=await authorized_for_room(sender,room)
        if not ok: return msg
        bots=load(BOTS_FILE,[]); targets=[b for b in bots if b.get('room_name','').lower()==room.lower()]
        for b in targets: await remove_bot(b)
        remaining=[b for b in bots if b not in targets]
        save(BOTS_FILE,remaining)
        if not remaining:
            masters=load(MASTERS_FILE,{})
            masters.pop(str(room_obj['id']),None)
            save(MASTERS_FILE,masters)
        return L(f'✅ تم حذف {len(targets)} بوت من {room}.',f'✅ Removed {len(targets)} bots from {room}.')
    if low.startswith('del@'):
        room=t.split('@',1)[1].strip(); ok,room_obj,msg=await authorized_for_room(sender,room)
        if not ok: return msg
        bots=load(BOTS_FILE,[]); targets=[b for b in bots if b.get('room_name','').lower()==room.lower() and b.get('role')=='main']
        for b in targets: await remove_bot(b)
        save(BOTS_FILE,[b for b in bots if b not in targets])
        return L(f'✅ تم حذف {len(targets)} Main Bot من {room}.',f'✅ Removed {len(targets)} Main Bot(s) from {room}.')
    if low.startswith('master@'):
        parts=t.split('@',2)
        if len(parts)!=3:
            return L('❌ الصيغة: master@اسم_المستخدم@الغرفة','❌ Format: master@username@room')
        target_name, room_name=parts[1].strip().lstrip('@'), parts[2].strip()
        ok,room,msg=await primary_master_for_room(sender,room_name)
        if not ok: return msg
        profile,err=await resolve_profile(target_name)
        if err: return L(f'❌ المستخدم غير موجود: {target_name}',f'❌ User not found: {target_name}')
        masters=load(MASTERS_FILE,{})
        rec=masters[str(room['id'])]
        arr=[str(x) for x in (rec.get('masters') or [])]
        if str(profile['id'])==str(rec.get('master_id')) or str(profile['id']) in arr:
            return L('⚠️ هذا المستخدم ماستر بالفعل.','⚠️ This user is already a master.')
        arr.append(str(profile['id']))
        rec['masters']=arr
        rec['master_names']=list(dict.fromkeys([*(rec.get('master_names') or []), str(profile.get('username') or target_name)]))
        rec['updated_at']=now(); masters[str(room['id'])]=rec; save(MASTERS_FILE,masters)
        return L(f'✅ تمت إضافة @{profile.get("username") or target_name} كماستر للغرفة {room["name"]}.',f'✅ @{profile.get("username") or target_name} was added as a room master for {room["name"]}.')

    if low.startswith('delmaster@'):
        parts=t.split('@',2)
        if len(parts)!=3:
            return L('❌ الصيغة: delmaster@اسم_المستخدم@الغرفة','❌ Format: delmaster@username@room')
        target_name, room_name=parts[1].strip().lstrip('@'), parts[2].strip()
        ok,room,msg=await primary_master_for_room(sender,room_name)
        if not ok: return msg
        profile,err=await resolve_profile(target_name)
        if err: return L(f'❌ المستخدم غير موجود: {target_name}',f'❌ User not found: {target_name}')
        masters=load(MASTERS_FILE,{})
        rec=masters[str(room['id'])]
        arr=[str(x) for x in (rec.get('masters') or [])]
        if str(profile['id']) not in arr:
            return L('⚠️ هذا المستخدم ليس ماستراً مضافاً.','⚠️ This user is not an added master.')
        rec['masters']=[x for x in arr if x != str(profile['id'])]
        rec['master_names']=[x for x in (rec.get('master_names') or []) if x.lower()!=str(profile.get('username') or target_name).lower()]
        rec['updated_at']=now(); masters[str(room['id'])]=rec; save(MASTERS_FILE,masters)
        return L(f'✅ تمت إزالة @{profile.get("username") or target_name} من ماسترات الغرفة.',f'✅ @{profile.get("username") or target_name} was removed from room masters.')

    if low.startswith('masters@'):
        room_name=t.split('@',1)[1].strip(); ok,room,msg=await authorized_for_room(sender,room_name)
        if not ok: return msg
        rec=load(MASTERS_FILE,{}).get(str(room['id']),{})
        names=[rec.get('master_name','')] + list(rec.get('master_names') or [])
        names=list(dict.fromkeys([n for n in names if n]))
        return L('👑 ماسترات الغرفة:\n'+'\n'.join(f'• @{n}' for n in names), '👑 Room masters:\n'+'\n'.join(f'• @{n}' for n in names))

    if low=='bots':
        bots=load(BOTS_FILE,[])
        masters=load(MASTERS_FILE,{})
        allowed_rooms={rid for rid,rec in masters.items() if str(rec.get('master_id',''))==str(sender) or str(sender) in [str(x) for x in (rec.get('masters') or [])]}
        bots=[b for b in bots if str(b.get('room_id','')) in allowed_rooms]
        if not bots: return L('🤖 لا توجد بوتات تحت تحكمك.','🤖 You have no controlled bots.')
        return L('🤖 بوتاتك:\n'+'\n'.join(f'• @{b["username"]} — {"Main Bot" if b.get("role")=="main" else "Hang Bot"} — {b.get("room_name","")} — رتبة: {b.get("rank","")} — {b.get("status","")}' for b in bots), '🤖 Your bots:\n'+'\n'.join(f'• @{b["username"]} — {"Main Bot" if b.get("role")=="main" else "Hang Bot"} — {b.get("room_name","")} — رتبة: {b.get("rank","")} — {b.get("status","")}' for b in bots))
    if low.startswith('room@'):
        name=t.split('@',1)[1].strip(); ok,room,msg=await authorized_for_room(sender,name)
        if not ok: return msg
        if not room: return L('❌ الغرفة غير موجودة.','❌ Room not found.')
        members=await select('room_members', 'user_id,rank', room_id=room['id'])
        return L(f'🏠 الغرفة: {room["name"]}\n🆔 {room["id"]}\n👥 الأعضاء: {len(members)}',f'🏠 Room: {room["name"]}\n🆔 {room["id"]}\n👥 Members: {len(members)}')
    if low.startswith('clean@'):
        room=t.split('@',1)[1].strip(); ok,r,msg=await authorized_for_room(sender,room)
        if not ok: return msg
        # Safe cleanup: only bot-control state for this room, not user messages.
        bots=load(BOTS_FILE,[]); targets=[b for b in bots if b.get('room_name','').lower()==room.lower()]
        for b in targets: await remove_bot(b)
        save(BOTS_FILE,[b for b in bots if b not in targets])
        masters=load(MASTERS_FILE,{})
        masters.pop(str(r['id']),None)
        save(MASTERS_FILE,masters)
        return L(f'🧹 تم تنظيف ذاكرة التحكم الخاصة بالغرفة {room}.',f'🧹 Control memory for {room} was cleaned.')
    return L('❓ أمر غير معروف. اكتب help.','❓ Unknown command. Type help.')

async def accept_pending_friend_requests():
    """Accept every pending incoming friendship request for the control bot."""
    rows=await asyncio.to_thread(lambda: sb.table('friendships').select('id,requester_id,addressee_id,status').eq('addressee_id',str(BOT_ID)).eq('status','pending').limit(100).execute().data or [])
    for row in rows:
        request_id=row.get('id'); requester=row.get('requester_id')
        if not request_id or not requester:
            continue
        updated=await run(lambda rid=request_id: sb.table('friendships').update({'status':'accepted'}).eq('id',rid).eq('status','pending').execute().data)
        if updated is None:
            log.warning('Could not accept friendship request %s from %s',request_id,requester)
            continue
        # Store a control-level language choice for the new friend.
        state=language_state()
        if str(requester) not in state.get('users',{}):
            set_pending_language(requester,'__control__','بوت التحكم')
            await send_dm(requester, language_prompt('بوت التحكم'))
        log.info('Accepted friendship request %s from %s',request_id,requester)

async def friend_loop():
    while True:
        try:
            await accept_pending_friend_requests()
        except Exception:
            log.exception('friend request loop failed')
        await asyncio.sleep(POLL)

async def presence_loop():
    """Keep the control bot online using the same profiles.last_seen_at field as the app."""
    while True:
        try:
            await run(lambda: sb.table('profiles').update({'last_seen_at':now()}).eq('id',str(BOT_ID)).execute().data)
        except Exception:
            log.exception('presence update failed')
        await asyncio.sleep(25)

async def dm_loop():
    global last_dm
    while True:
        try:
            rows=await asyncio.to_thread(lambda: sb.table('dm_relay').select('*').gt('created_at',last_dm).order('created_at').limit(50).execute().data or [])
            for row in rows:
                last_dm=row['created_at']; sender=row.get('sender_id'); env=row.get('envelope') or {}; text=str(env.get('content') or '').strip()
                if not text or sender==BOT_ID: continue
                reply=await control_action(str(sender),text)
                envelope={'v':1,'id':str(uuid.uuid4()),'content':reply,'message_type':'text','media_url':None,'media_duration_ms':None,'reply_to_id':None,'created_at':now()}
                await asyncio.to_thread(lambda s=sender,e=envelope: sb.table('dm_relay').insert({'sender_id':BOT_ID,'recipient_id':s,'envelope':e}).execute())
        except Exception: log.exception('dm loop')
        await asyncio.sleep(POLL)

async def room_loop():
    # The control bot does not execute control commands from public rooms by default; it only manages through DM.
    while True: await asyncio.sleep(60)

async def main():
    global BOT_ID,last_dm
    log.info('Starting control bot version %s', CONTROL_BOT_VERSION)
    email=await resolve_email(sb,CONTROL_USERNAME)
    try:
        # Keep the public configuration as username/password. The email here
        # is only the internal Supabase mapping and is never requested from the user.
        res=await asyncio.to_thread(
            lambda: sb.auth.sign_in_with_password(
                {'email': email, 'password': CONTROL_PASSWORD}
            )
        )
    except Exception as exc:
        # Do not hide the actual Supabase response behind a generic line-503 error.
        log.error('Supabase login failed for username %s: %s', CONTROL_USERNAME, exc)
        raise RuntimeError(
            'Control bot login failed: Supabase rejected the username/password. '
            'Check GIANT_USERNAME and GIANT_PASSWORD in the deployment variables.'
        ) from exc
    if not res or not getattr(res,'user',None):
        raise RuntimeError(
            'Control bot login failed: Supabase returned no user. '
            'Check GIANT_USERNAME and GIANT_PASSWORD in the deployment variables.'
        )
    BOT_ID=res.user.id; log.info('Control bot connected as @%s',CONTROL_USERNAME)
    # Restart persisted bots automatically.
    for b in load(BOTS_FILE,[]):
        try:
            task=asyncio.create_task(child_runner(b),name=f'bot-{b.get("username")}'); child_tasks[b['id']]=task
        except Exception: log.exception('child start failed')
    await asyncio.gather(dm_loop(), friend_loop(), presence_loop(), room_loop())

if __name__=='__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass

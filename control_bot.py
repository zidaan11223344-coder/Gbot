import os, json, uuid, asyncio, logging, re, time
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

BASE=Path(__file__).resolve().parent
# Read variables from .env; existing environment variables remain supported.
load_dotenv(BASE/'.env', override=False)
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
POLL=max(0.15, float(os.environ.get('CONTROL_POLL_SECONDS','0.20')))
CONTROL_BOT_VERSION='username-login-v6-room-isolation-rejoin-status'
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
# Per-sender/per-room pagination for .r/.nx. It is intentionally in-memory so
# every server restart starts from the current live room membership again.
ROOM_MEMBER_PAGES={}
# In-memory pending interactive commands. Keyed by room + sender so the
# username sent as the next message is never lost between DB polling cycles.
PENDING_COMMANDS={}

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

async def ensure_bot_in_room(client, room):
    """Ensure a managed bot is actually a member of its configured room.

    A moderator/admin can remove the bot from the room while its login session
    remains alive. In that case member_rank() becomes None. We immediately
    re-join the bot and verify membership again instead of waiting for a full
    reconnect. This is used for both Main control bots and silent/hang bots.
    """
    uid=client_user_id(client)
    if not uid or not room or not room.get('id'):
        return None, False

    rank=await member_rank(client,room['id'],uid)
    if rank is not None:
        return rank, False

    log.warning('BOT REMOVED/MISSING: rejoining @%s to room=%s',
                uid,room.get('name') or room.get('id'))
    try:
        joined=await join_bot(client,room)
        if joined is None:
            return None, True
    except Exception as exc:
        log.warning('automatic rejoin failed room=%s user=%s: %s',room.get('id'),uid,exc)
        return None, True

    # Give the database/RPC a moment to commit the membership, then verify it.
    await asyncio.sleep(0.25)
    rank=await member_rank(client,room['id'],uid)
    if rank is None:
        log.warning('automatic rejoin not confirmed yet for @%s room=%s',
                    uid,room.get('name') or room.get('id'))
        return None, True

    log.info('AUTOREJOIN OK: @%s rejoined room=%s rank=%s',
             uid,room.get('name') or room.get('id'),rank)
    return rank, True

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
# Track members already seen by each running Main bot so welcome messages
# are sent only for newly observed members, not everyone on startup.
ROOM_WELCOME_SEEN_STATE = BASE / "room_welcome_seen.json"
ROOM_UNDO_STATE = BASE / "room_undo.json"
ROOM_BOT_GREETING_STATE = BASE / "room_bot_greeting_seen.json"

def _room_welcome_seen():
    x=load(ROOM_WELCOME_SEEN_STATE,{})
    return x if isinstance(x,dict) else {}

def _save_room_welcome_seen(x): save(ROOM_WELCOME_SEEN_STATE,x)

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

def _room_bot_greeting_seen():
    x=load(ROOM_BOT_GREETING_STATE,{})
    return x if isinstance(x,dict) else {}

def _save_room_bot_greeting_seen(x): save(ROOM_BOT_GREETING_STATE,x)

def _room_undo():
    x=load(ROOM_UNDO_STATE,{})
    return x if isinstance(x,dict) else {}

def _save_room_undo(x): save(ROOM_UNDO_STATE,x)

def _set_room_undo(room_id, item):
    x=_room_undo(); x[str(room_id)]=item; _save_room_undo(x)

def _pop_room_undo(room_id):
    x=_room_undo(); item=x.pop(str(room_id),None); _save_room_undo(x); return item

async def room_username(client, uid):
    try:
        rows=await asyncio.to_thread(lambda: client.table('profiles').select('username').eq('id',str(uid)).limit(1).execute().data or [])
        return str(rows[0].get('username') or uid) if rows else str(uid)
    except Exception:
        return str(uid)

async def child_profile(client, username):
    # Accept @username, surrounding spaces, and case differences.
    clean=str(username or "").strip().lstrip("@").strip()
    if not clean: return None
    try:
        def lookup():
            rows=client.table('profiles').select('id,username').eq('username',clean).limit(1).execute().data or []
            if rows:
                return rows[0]
            # PostgreSQL/Supabase supports ilike; this avoids a false "user not found"
            # when the client sends a different username case.
            rows=client.table('profiles').select('id,username').ilike('username',clean).limit(1).execute().data or []
            return rows[0] if rows else None
        return await asyncio.to_thread(lookup)
    except Exception as exc:
        log.warning("profile lookup failed for %r: %s", clean, exc)
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
    """Strict per-room authorization.

    A user is allowed to control a bot ONLY when that user is registered for
    this exact room as the primary master, an added room master, or an explicit
    bot-control master.  Giant rank in another room is never considered here.
    This is the critical isolation boundary between rooms.
    """
    rec=load(MASTERS_FILE,{}).get(str(room_id))
    if not rec:
        return False
    sid=str(uid)
    primary=str(rec.get('master_id',''))
    room_masters=[str(x) for x in (rec.get('masters') or [])]
    control_masters=[str(x) for x in (rec.get('control_masters') or [])]
    return sid in ({primary} | set(room_masters) | set(control_masters))

async def child_target_id(client, username):
    p=await child_profile(client,username)
    return (str(p.get('id')), p.get('username') or username) if p else (None,None)

async def child_moderate(client, room_id, action, target_username, minutes=0):
    tid,tname=await child_target_id(client,target_username)
    if not tid:
        return False, f"❌ المستخدم @{str(target_username).lstrip('@')} غير موجود."
    if tid == client_user_id(client):
        return False, "❌ لا يمكن للبوت تنفيذ الإجراء على نفسه."

    rpc_args={'_room':room_id,'_user':tid}
    # Keep the last reversible moderation/rank operation per room.
    previous_rank=await member_rank(client,room_id,tid)
    if action in ('rank_moderator','rank_owner','rank_member','rank_admin'):
        # A promotion must also work for a currently banned user. The unban is
        # intentionally attempted first and its result is ignored; if the user
        # was not banned, the subsequent rank operation still proceeds.
        await child_rpc(client,'unban_room_member',{'_room':room_id,'_user':tid})

    if action == 'kick':
        data=await child_rpc(client,'kick_room_member',rpc_args)
    elif action == 'ban':
        rpc_args['reason']='إجراء إداري عبر بوت التحكم'
        data=await child_rpc(client,'ban_room_member',rpc_args)
    elif action == 'unban':
        data=await child_rpc(client,'unban_room_member',rpc_args)
    elif action == 'mute':
        rpc_args['minutes']=int(minutes or 5)
        data=await child_rpc(client,'mute_room_member',rpc_args)
    elif action in ('rank_moderator','rank_owner','rank_member','rank_admin'):
        rank_map={'rank_moderator':'moderator','rank_owner':'owner','rank_member':'member','rank_admin':'admin'}
        rpc_args['_new_rank']=rank_map[action]
        data=await child_rpc(client,'set_member_rank',rpc_args)
    else:
        return False, "❌ إجراء غير مدعوم."
    if data is None:
        return False, "❌ رفض Giant الإجراء أو لم تُرجع قاعدة البيانات نتيجة."

    if action in ('ban','rank_moderator','rank_owner','rank_member','rank_admin'):
        _set_room_undo(room_id,{
            'action':action,'user_id':str(tid),'username':str(tname),
            'previous_rank':previous_rank or 'member','created_at':now()
        })
    return True, tname

def child_filter_words(room_id):
    st=_room_state()
    item=st.setdefault(str(room_id),{'filter':False,'words':[],'muted':{},'pending':{}})
    return item

async def child_handle_admin(client, rec, room_id, sender_id, text, from_dm=False):
    t=str(text or "").strip()
    low=norm(t)
    if not t: return None
    # Check the exact room authorization before doing any profile lookup.
    # This keeps command response latency low and preserves strict room isolation.
    if not await child_is_room_master(client, room_id, sender_id):
        if from_dm:
            return "🚫 هذا البوت يقبل أوامر هذه الغرفة من الماستر المعيّن فقط."
        return None

    sender_name=await room_username(client,sender_id)

    state=child_filter_words(room_id)
    replies=_room_replies()
    welcome=_room_welcome()

    # Interactive commands are keyed by BOTH room and sender.  Handle a
    # pending username before any normal command/reply processing so a plain
    # message such as "احمد" is always consumed as the requested target.
    pending=state.setdefault('pending',{})
    pending_key=f'{room_id}:{sender_id}'
    pending_item=PENDING_COMMANDS.get(pending_key) or pending.get(pending_key)
    if pending_item:
        try:
            age=time.time()-float(pending_item.get('created_at',0))
        except Exception:
            age=999999
        if low in ('الغاء','إلغاء','cancel'):
            PENDING_COMMANDS.pop(pending_key,None)
            pending.pop(pending_key,None)
            _save_room_state(state)
            return L('✅ تم إلغاء الأمر المعلّق.','✅ Pending command cancelled.',room_id=room_id)
        if age <= 120 and str(pending_item.get('room_id') or room_id)==str(room_id):
            p=pending_item
            target=t.strip().lstrip('@').strip()
            if not target:
                return L('✍️ أرسل اسم المستخدم.','✍️ Send the username.',room_id=room_id)
            # Usernames are single tokens in Giant. Accept plain "احمد" and
            # optional leading @; ignore accidental trailing text.
            target=target.split()[0].lstrip('@')
            log.info('PENDING EXEC room=%s sender=%s action=%s target=%r',room_id,sender_id,p.get('action'),target)
            PENDING_COMMANDS.pop(pending_key,None)
            pending.pop(pending_key,None)
            if p['action'] in ('control_master_add','control_master_remove'):
                profile=await child_profile(client,target)
                if not profile:
                    _save_room_state(state)
                    return L(f'❌ المستخدم @{target} غير موجود.',f'❌ User @{target} was not found.',room_id=room_id)
                allm=load(MASTERS_FILE,{})
                recm=allm.get(str(room_id)) or {}
                arr=[str(x) for x in (recm.get('control_masters') or [])]
                pid=str(profile.get('id')); pname=str(profile.get('username') or target)
                if p['action']=='control_master_add':
                    if pid in arr:
                        _save_room_state(state)
                        return L(f'⚠️ @{pname} ماستر بالفعل.',f'⚠️ @{pname} is already a bot-control master.',room_id=room_id)
                    arr.append(pid); recm['control_masters']=arr
                    recm['control_master_names']=list(dict.fromkeys([*(recm.get('control_master_names') or []),pname]))
                    recm['updated_at']=now(); allm[str(room_id)]=recm; save(MASTERS_FILE,allm)
                    _save_room_state(state)
                    return L(f'✅ تم إعطاء @{pname} ماستر تحكم بالبوت المتحدث.',f'✅ @{pname} can now control the speaking bot.',room_id=room_id)
                if pid not in arr:
                    _save_room_state(state)
                    return L(f'⚠️ @{pname} ليس ماستر تحكم.',f'⚠️ @{pname} is not a bot-control master.',room_id=room_id)
                recm['control_masters']=[x for x in arr if x!=pid]
                recm['control_master_names']=[x for x in (recm.get('control_master_names') or []) if x.lower()!=pname.lower()]
                recm['updated_at']=now(); allm[str(room_id)]=recm; save(MASTERS_FILE,allm)
                _save_room_state(state)
                return L(f'✅ تمت إزالة ماستر التحكم @{pname}.',f'✅ Bot-control master @{pname} was removed.',room_id=room_id)

            if p['action']=='mute':
                parts=t.split()
                minutes=int(parts[1]) if len(parts)>1 and parts[1].isdigit() else 5
                ok,msg=await child_moderate(client,room_id,'mute',target,minutes)
            elif p['action'] in ('rank_moderator','rank_owner','rank_admin','rank_member','unban'):
                ok,msg=await child_moderate(client,room_id,p['action'],target)
            elif p['action']=='unmute':
                tid,tname=await child_target_id(client,target)
                ok,msg=(False,'المستخدم غير موجود.') if not tid else (True,tname)
                if ok:
                    rs=child_filter_words(room_id); rs.setdefault('muted',{}).pop(str(tid),None); _save_room_state(_room_state())
                    data=await child_rpc(client,'unmute_room_member',{'_room':room_id,'_user':tid})
                    if data is None: ok=False; msg='رفض Giant فك الكتم.'
            else:
                ok,msg=await child_moderate(client,room_id,p['action'],target)
            _save_room_state(state)
            return (L(f'✅ تم تنفيذ الأمر على @{msg}.',f'✅ Command executed for @{msg}.',room_id=room_id) if ok else str(msg))
        # Expired pending command. Clear it and continue normally.
        PENDING_COMMANDS.pop(pending_key,None)
        pending.pop(pending_key,None)
        _save_room_state(state)

    # Room/user information commands.
    # .r = current users in THIS room; .is = user status/rooms.
    # Legacy management views remain available as .gr and .gis.
    if low == '.r' or low == '.rooms':
        return await command_room_members(client, room_id, sender_id, next_page=False)
    if low == '.nx':
        return await command_room_members(client, room_id, sender_id, next_page=True)

    if low == '.is' or low.startswith('.is '):
        return await command_user_info(t)

    if low == '.gr' or low == '.gr ' or low == '.roomsall':
        return await command_rooms_list(sender)

    if low == '.gis' or low.startswith('.gis '):
        return await command_bot_info(sender,t)

    if low in ('help','مساعدة','الاوامر','الأوامر'):
        return L(
            "🛡️ أوامر بوت التحكم:\n"
            "a@اسم — إشراف\n"
            "o@اسم — أونر\n"
            "ad@اسم — أدمن\n"
            "m@اسم — عضو\n"
            "b@اسم — حظر\n"
            "u@اسم — فك حظر\n"
            "k@اسم — طرد\n"
            "حظر / طرد / فك حظر / رفع إشراف / رفع أونر / رفع عضو → ثم اسم المستخدم\n"
            "mas@اسم — إعطاء ماستر تحكم بالبوت\n"
            "umas@اسم — إزالة ماستر التحكم\n"
            ".u — التراجع عن آخر حظر أو ترقية\n"
            "كتم / فك الكتم → ثم اسم المستخدم\n"
            "+wc نص الترحيب\n"
            "wc@on / wc@off / l@wc / clear@wc\n"
            ".is اسم_المستخدم — حالة المستخدم والغرف الموجودة فيها\n"
            ".r — جميع المستخدمين الموجودين في هذه الغرفة\n"
            ".gis اسم_البوت — حالة البوت\n"
            ".gr — جميع الغرف والبوتات\n"
            "استخدم (user) أو {user} أو {username} داخل الترحيب",
            "🛡️ Control bot commands:\n"
            "a@username — moderator\n"
            "o@username — owner\n"
            "ad@username — admin\n"
            "m@username — member\n"
            "b@username — ban\n"
            "u@username — unban\n"
            "k@username — kick\n"
            "ban / kick / unban / promote moderator / owner / member → then username\n"
            "mas@username — grant bot-control master\n"
            "umas@username — remove bot-control master\n"
            ".u — undo the last ban or rank change\n"
            "mute / unmute → then username\n"
            "+wc welcome text\n"
            "wc@on / wc@off / l@wc / clear@wc\n"
            ".is username — user status and rooms\n"
            ".r — all current users in this room\n"
            ".gis username — managed-bot status\n"
            ".gr — all managed rooms and bots\n"
            "Use (user), {user}, or {username} in welcome text",
            room_id=room_id
        )

    # Compact room commands requested for fast moderation.
    # Check ad@ before a@ because both start with the letter 'a'.
    if low.startswith('ad@'):
        target=t[3:].strip().lstrip('@')
        if target:
            ok,msg=await child_moderate(client,room_id,'rank_admin',target)
            if ok:
                return L(f'✅ تم الأدمن لـ @{msg}.',f'✅ @{msg} was promoted to admin.',room_id=room_id)
            return msg

    # a@user=moderator, o@user=owner, m@user=member,
    # k@user=kick, b@user=ban, u@user=unban. Upper/lower case are accepted.
    short_actions={
        'a':'rank_moderator','o':'rank_owner','m':'rank_member','d':'rank_admin',
        'k':'kick','b':'ban','u':'unban'
    }
    if len(t)>2 and t[1]=='@' and t[0].lower() in short_actions:
        action=short_actions[t[0].lower()]
        target=t[2:].strip().lstrip('@')
        if target:
            ok,msg=await child_moderate(client,room_id,action,target)
            if ok:
                labels_ar={'rank_moderator':'الإشراف','rank_owner':'الأونر','rank_admin':'الأدمن','rank_member':'العضو','kick':'الطرد','ban':'الحظر','unban':'فك الحظر'}
                labels_en={'rank_moderator':'moderator','rank_owner':'owner','rank_admin':'admin','rank_member':'member','kick':'kick','ban':'ban','unban':'unban'}
                return L(f"✅ تم {labels_ar[action]} لـ @{msg}.",f"✅ @{msg} was changed to {labels_en[action]}.",room_id=room_id)
            return msg

    # .u reverses the last ban or rank operation performed by this bot in the room.
    if low=='.u':
        item=_pop_room_undo(room_id)
        if not item:
            return L('⚠️ لا يوجد أمر قابل للتراجع عنه.','⚠️ There is no reversible command to undo.',room_id=room_id)
        tid=str(item.get('user_id') or '')
        target=item.get('username') or tid
        if item.get('action')=='ban':
            data=await child_rpc(client,'unban_room_member',{'_room':room_id,'_user':tid})
            if data is None:
                return L(f'❌ تعذر التراجع عن حظر @{target}.',f'❌ Could not undo the ban on @{target}.',room_id=room_id)
            return L(f'↩️ تم التراجع عن حظر @{target}.',f'↩️ Ban on @{target} was undone.',room_id=room_id)
        previous=item.get('previous_rank') or 'member'
        data=await child_rpc(client,'set_member_rank',{'_room':room_id,'_user':tid,'_new_rank':previous})
        if data is None:
            return L(f'❌ تعذر التراجع عن ترقية @{target}.',f'❌ Could not undo the rank change for @{target}.',room_id=room_id)
        return L(f'↩️ تم التراجع عن ترقية @{target} وإعادته إلى {previous}.',f'↩️ Rank change for @{target} was undone; restored to {previous}.',room_id=room_id)

    # Give/remove a room-control master for the speaking Main bot.
    if low.startswith('mas@') or low.startswith('umas@'):
        add_master=low.startswith('mas@')
        target=t.split('@',1)[1].strip().lstrip('@')
        if not target:
            return L('❌ الصيغة: mas@اسم_المستخدم أو umas@اسم_المستخدم','❌ Format: mas@username or umas@username',room_id=room_id)
        profile=await child_profile(client,target)
        if not profile:
            return L(f'❌ المستخدم @{target} غير موجود.',f'❌ User @{target} was not found.',room_id=room_id)
        recm=load(MASTERS_FILE,{}).get(str(room_id)) or {}
        arr=[str(x) for x in (recm.get('control_masters') or [])]
        pid=str(profile.get('id'))
        if add_master:
            if pid in arr:
                return L(f'⚠️ @{profile.get("username") or target} ماستر بالفعل.',f'⚠️ @{profile.get("username") or target} is already a bot-control master.',room_id=room_id)
            arr.append(pid); recm['control_masters']=arr
            recm['control_master_names']=list(dict.fromkeys([*(recm.get('control_master_names') or []),str(profile.get('username') or target)]))
            recm['updated_at']=now(); allm=load(MASTERS_FILE,{}); allm[str(room_id)]=recm; save(MASTERS_FILE,allm)
            return L(f'✅ تم إعطاء @{profile.get("username") or target} ماستر تحكم بالبوت المتحدث.',f'✅ @{profile.get("username") or target} can now control the speaking bot.',room_id=room_id)
        if pid not in arr:
            return L(f'⚠️ @{profile.get("username") or target} ليس ماستر تحكم.',f'⚠️ @{profile.get("username") or target} is not a bot-control master.',room_id=room_id)
        recm['control_masters']=[x for x in arr if x!=pid]
        recm['control_master_names']=[x for x in (recm.get('control_master_names') or []) if x.lower()!=str(profile.get('username') or target).lower()]
        recm['updated_at']=now(); allm=load(MASTERS_FILE,{}); allm[str(room_id)]=recm; save(MASTERS_FILE,allm)
        return L(f'✅ تمت إزالة ماستر التحكم @{profile.get("username") or target}.',f'✅ Bot-control master @{profile.get("username") or target} was removed.',room_id=room_id)

    # Interactive moderation like bot.py.
    pending=state.setdefault('pending',{})
    pending_key=f'{room_id}:{sender_id}'
    if low in ('حظر','طرد','كتم','فك الكتم','فك_الكتم','unmute','فك حظر','فك_الحظر'):
        action=('ban' if low=='حظر' else 'kick' if low=='طرد' else 'mute' if low=='كتم' else 'unmute' if low in ('فك الكتم','فك_الكتم','unmute') else 'unban')
        pending[pending_key]={'action':action,'room_id':str(room_id),'created_at':time.time()}
        PENDING_COMMANDS[pending_key]=dict(pending[pending_key])
        log.info('PENDING SET room=%s sender=%s action=%s',room_id,sender_id,action)
        _save_room_state(state)
        return L(f"✍️ أرسل اسم المستخدم لتنفيذ «{t}».",f"✍️ Send the username to execute “{t}”.",room_id=room_id)

    # Restore room-role management commands.
    # Arabic aliases are kept alongside English aliases for compatibility.
    role_commands = {
        'رفع اشراف': 'rank_moderator', 'رفع إشراف': 'rank_moderator',
        'رفع مشرف': 'rank_moderator', 'رفع مراقب': 'rank_moderator',
        'رفع اونر': 'rank_owner', 'رفع أونر': 'rank_owner',
        'رفع مالك': 'rank_owner', 'رفع ادمن': 'rank_admin', 'رفع أدمن': 'rank_admin', 'رفع عضو': 'rank_member',
        'اشراف': 'rank_moderator', 'إشراف': 'rank_moderator',
        'اونر': 'rank_owner', 'أونر': 'rank_owner', 'ادمن': 'rank_admin', 'أدمن': 'rank_admin', 'عضو': 'rank_member',
    }
    interactive_aliases={
        'اشراف':'rank_moderator','إشراف':'rank_moderator','اونر':'rank_owner','أونر':'rank_owner',
        'ادمن':'rank_admin','أدمن':'rank_admin','عضو':'rank_member',
        'ماستر':'control_master_add','ازاله ماستر':'control_master_remove','إزالة ماستر':'control_master_remove'
    }
    if low in interactive_aliases:
        pending[pending_key]={'action':interactive_aliases[low],'room_id':str(room_id),'created_at':time.time()}
        PENDING_COMMANDS[pending_key]=dict(pending[pending_key])
        log.info('PENDING SET room=%s sender=%s action=%s',room_id,sender_id,interactive_aliases[low])
        _save_room_state(state)
        return L(f"✍️ أرسل اسم المستخدم لتنفيذ «{t}».",f"✍️ Send the username to execute “{t}”.",room_id=room_id)

    if low in ('فك الحظر','فك_الحظر','تراجع عن الحظر','تراجع عن حظر','unban','unban@'):
        pending[pending_key]={'action':'unban','room_id':str(room_id),'created_at':time.time()}
        PENDING_COMMANDS[pending_key]=dict(pending[pending_key])
        log.info('PENDING SET room=%s sender=%s action=unban',room_id,sender_id)
        _save_room_state(state)
        return L("✍️ أرسل اسم المستخدم لفك الحظر.","✍️ Send the username to unban.",room_id=room_id)
    if low in role_commands:
        pending[pending_key]={'action':role_commands[low],'room_id':str(room_id),'created_at':time.time()}
        PENDING_COMMANDS[pending_key]=dict(pending[pending_key])
        log.info('PENDING SET room=%s sender=%s action=%s',room_id,sender_id,role_commands[low])
        _save_room_state(state)
        return L(f"✍️ أرسل اسم المستخدم لتنفيذ «{t}».",f"✍️ Send the username to execute “{t}”.",room_id=room_id)

    # Direct role/unban syntax: رفع اشراف @user / رفع اونر @user / رفع عضو @user
    direct_role = [
        ('رفع اشراف','rank_moderator'),('رفع إشراف','rank_moderator'),
        ('رفع مشرف','rank_moderator'),('رفع اونر','rank_owner'),
        ('رفع أونر','rank_owner'),('رفع مالك','rank_owner'), ('رفع ادمن','rank_admin'), ('رفع أدمن','rank_admin'),
        ('رفع عضو','rank_member'),('unban','unban'),
        ('فك الحظر','unban'),('فك_الحظر','unban'),('تراجع عن الحظر','unban'),
        ('تراجع عن حظر','unban')
    ]
    for prefix, action in direct_role:
        if low.startswith(prefix+' '):
            target=t[len(prefix):].strip().lstrip('@')
            if not target: return L(f"استخدم: {prefix} @اسم_المستخدم",f"Use: {prefix} @username",room_id=room_id)
            ok,msg=await child_moderate(client,room_id,action,target)
            if ok:
                labels={'rank_moderator':'الإشراف','rank_owner':'الأونر','rank_admin':'الأدمن','rank_member':'العضو','unban':'فك الحظر'}
                return L(f"✅ تم {labels[action]} لـ @{msg}.",f"✅ @{msg} was updated to {action.replace('rank_','')}.",room_id=room_id)
            return msg
        if low.startswith(prefix+'@'):
            target=t.split('@',1)[1].strip()
            ok,msg=await child_moderate(client,room_id,action,target)
            if ok:
                labels={'rank_moderator':'الإشراف','rank_owner':'الأونر','rank_admin':'الأدمن','rank_member':'العضو','unban':'فك الحظر'}
                return L(f"✅ تم {labels[action]} لـ @{msg}.",f"✅ @{msg} was updated to {action.replace('rank_','')}.",room_id=room_id)
            return msg

    # Direct moderation syntax also accepts spaces: حظر @user / طرد @user / كتم @user 5
    if low.startswith('حظر ') or low.startswith('ban '):
        target=t.split(None,1)[1].strip().lstrip('@') if len(t.split(None,1))>1 else ''
        if not target: return L('استخدم: حظر @اسم_المستخدم','Use: ban @username',room_id=room_id)
        ok,msg=await child_moderate(client,room_id,'ban',target)
        return (L(f'✅ تم حظر @{msg}.',f'✅ @{msg} was banned.',room_id=room_id) if ok else msg)
    if low.startswith('طرد ') or low.startswith('kick '):
        target=t.split(None,1)[1].strip().lstrip('@') if len(t.split(None,1))>1 else ''
        if not target: return L('استخدم: طرد @اسم_المستخدم','Use: kick @username',room_id=room_id)
        ok,msg=await child_moderate(client,room_id,'kick',target)
        return (L(f'✅ تم طرد @{msg}.',f'✅ @{msg} was kicked.',room_id=room_id) if ok else msg)
    if low.startswith('كتم ') or low.startswith('mute '):
        parts=t.split()
        if len(parts)<2: return L('استخدم: كتم @اسم_المستخدم [الدقائق]','Use: mute @username [minutes]',room_id=room_id)
        target=parts[1].lstrip('@'); minutes=int(parts[2]) if len(parts)>2 and parts[2].isdigit() else 5
        ok,msg=await child_moderate(client,room_id,'mute',target,minutes)
        return (L(f'✅ تم كتم @{msg} لمدة {minutes} دقيقة.',f'✅ @{msg} was muted for {minutes} minutes.',room_id=room_id) if ok else msg)

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

async def child_send_join_greeting(client, room_id):
    """Send the Main bot's one-time room greeting after it is accepted."""
    # Do not let the bot enter with the wrong language. The greeting is sent
    # only after a language has explicitly been selected for this room.
    lang_state=language_state()
    if str(room_id) not in lang_state.get('rooms',{}):
        return
    seen=_room_bot_greeting_seen()
    key=str(room_id)
    if seen.get(key):
        return
    text='مرحبا بكم في سيرفر جانيت' if lang(room_id=room_id)!='en' else 'Welcome to Janet Server'
    if await child_send_room(client,room_id,text):
        seen[key]={'sent_at':now()}
        _save_room_bot_greeting_seen(seen)

async def child_welcome_new_members(client, room_id):
    """Fallback member-diff welcome detector. Rank changes never trigger it."""
    welcome=_room_welcome()
    item=welcome.get(str(room_id),{})
    if not item.get('enabled') or not item.get('messages'):
        return
    try:
        rows=await asyncio.to_thread(
            lambda: client.table('room_members').select('user_id').eq('room_id',room_id).execute().data or []
        )
    except Exception as exc:
        log.warning('welcome member lookup failed room=%s: %s',room_id,exc)
        return
    seen_all=_room_welcome_seen()
    seen=set(str(x) for x in (seen_all.get(str(room_id)) or []))
    current=set(str(r.get('user_id')) for r in rows if r.get('user_id'))
    # On the very first run, initialize the baseline silently. Subsequent
    # joins are detected by membership appearing after the baseline.
    if str(room_id) not in seen_all:
        seen_all[str(room_id)]=sorted(current)
        _save_room_welcome_seen(seen_all)
        return
    new_ids=[uid for uid in current if uid not in seen]
    seen.intersection_update(current)
    for uid in new_ids:
        await child_send_welcome_for_user(client,room_id,uid,item)
    seen_all[str(room_id)]=sorted(current)
    _save_room_welcome_seen(seen_all)

async def child_send_welcome_for_user(client, room_id, uid, item=None):
    if item is None:
        item=_room_welcome().get(str(room_id),{})
    if not item.get('enabled') or not item.get('messages'):
        return False
    username=await room_username(client,uid)
    sent=False
    for template in item.get('messages',[]):
        msg=str(template)
        msg=msg.replace('(user)',f'@{username}').replace('{user}',f'@{username}').replace('{username}',str(username))
        sent = (await child_send_room(client,room_id,msg)) or sent
    return sent

async def extract_joined_user_id_async(client, message, room_id):
    uid=extract_joined_user_id(message,room_id)
    if uid:
        return uid
    content=str((message or {}).get('content') or '').strip()
    # Common Giant system-event forms: "دخل [username]", "joined [username]",
    # or an @username mention. Resolve the visible username through profiles.
    m=re.search(r'(?:دخل|انضم|joined|join)\s*\[([^\]]+)\]',content,re.I)
    if not m:
        m=re.search(r'@([A-Za-z0-9_.-]+)',content)
    if m:
        profile=await child_profile(client,m.group(1).strip())
        if profile:
            return str(profile.get('id'))
    return None

def extract_joined_user_id(message, room_id):
    """Best-effort extraction of a user id from Giant's green join system event.

    Different Giant builds store system events differently, so this helper only
    returns a candidate when a structured user id is present in the row.
    """
    if not isinstance(message,dict): return None
    for key in ('joined_user_id','member_id','target_user_id','affected_user_id'):
        value=message.get(key)
        if value:
            return str(value)
    meta=message.get('metadata') or message.get('meta') or {}
    if isinstance(meta,dict):
        for key in ('user_id','joined_user_id','member_id','target_user_id'):
            value=meta.get(key)
            if value:
                return str(value)
    return None

async def child_room_loop(rec):
    client=child_clients.get(rec['id'])
    room_id=rec.get('room_id')
    if not client or not room_id: return

    # Use a small timestamp overlap plus message-id deduplication.  The old
    # strict `.gt(created_at, cursor)` could miss the second message when two
    # messages were written with the same database timestamp (exactly the
    # "حظر -> username" sequence shown in the screenshot).
    cursor=now()
    seen=set()
    while True:
        try:
            # Process room messages first. The fallback membership-diff welcome
            # lookup is intentionally after commands so it can never add a DB
            # round-trip in front of a moderation command.
            rows=await asyncio.to_thread(
                lambda: client.table('room_messages').select('*')
                .eq('room_id',room_id).gte('created_at',cursor)
                .order('created_at').limit(100).execute().data or []
            )
            newest=cursor
            for m in rows:
                created=str(m.get('created_at') or '')
                mid=str(m.get('id') or '')
                uid=str(m.get('user_id') or '')
                content=str(m.get('content') or '').strip()
                # Prefer the DB id.  If an old schema has no id, the composite
                # key still prevents duplicate processing caused by gte().
                key=mid or f'{created}|{uid}|{m.get("message_type")}|{content}'
                if key in seen:
                    continue
                seen.add(key)
                if len(seen)>1000:
                    seen=set(list(seen)[-500:])
                if created and created>newest:
                    newest=created

                # Giant emits joins/promotions as system messages. Promotions
                # must never trigger welcome; only a join event may do so.
                if m.get('message_type')=='system':
                    event_type=str(m.get('event_type') or m.get('type') or '').lower()
                    joined_uid=await extract_joined_user_id_async(client,m,room_id)
                    is_join=('join' in event_type or 'دخل' in content or 'انضم' in content or 'joined' in content)
                    is_promotion=('rank' in event_type or 'ترقي' in content or ('عضو' in content and 'رفع' in content))
                    if is_join and not is_promotion and joined_uid and joined_uid != str(client_user_id(client)):
                        item=_room_welcome().get(str(room_id),{})
                        await child_send_welcome_for_user(client,room_id,joined_uid,item)
                    continue
                if not uid or uid==str(client_user_id(client)):
                    continue
                if not content:
                    continue
                log.info('ROOM MESSAGE room=%s sender=%s text=%r',room_id,uid,content)
                try:
                    reply=await child_handle_admin(client,rec,room_id,uid,content,from_dm=False)
                    if reply:
                        await child_send_room(client,room_id,reply)
                except Exception:
                    log.exception('command failed room=%s sender=%s text=%r',room_id,uid,content)
                    await child_send_room(client,room_id,'❌ حدث خطأ أثناء تنفيذ الأمر. راجع سجل البوت.')
            if newest!=cursor:
                cursor=newest
            # Fallback only: explicit Giant join events above are handled
            # immediately. This DB membership diff runs after command handling
            # so welcome detection cannot delay normal commands.
            await child_welcome_new_members(client, room_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('child room loop failed for %s',rec.get('username'))
        await asyncio.sleep(POLL)

async def child_dm_loop(rec):
    client=child_clients.get(rec['id'])
    room_id=rec.get('room_id')
    if not client: return
    # Small startup overlap: capture a DM that arrived during login/task startup.
    # The server's created_at cursor prevents older messages from being scanned
    # indefinitely, while the short overlap removes the visible startup gap.
    last=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
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
        await asyncio.sleep(POLL)

# ============================================================================
# [قسم تحكم الغرف المنقول من bot.py] END
# ============================================================================

async def _child_runner_once(rec):
    """Run one bot session. The outer child_runner reconnects after transient failures."""
    bid=rec['id']; username=rec['username']; password=rec['password']
    role=rec.get('role','main'); room_id=rec.get('room_id'); room_name=rec.get('room_name','')
    client=None
    room_task=None
    dm_task=None
    try:
        client,err=await login_client(username,password)
        if err:
            raise RuntimeError(err)

        child_clients[bid]=client
        rec['status']='online'; rec['error']=''; rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))

        room={'id':room_id,'name':room_name}
        joined=await join_bot(client,room)
        if joined is None:
            raise RuntimeError('room_join failed')

        rank = await member_rank(client, room_id, client_user_id(client))
        rec['rank']=rank or 'unknown'

        # Main/control bots must remain Moderator/Admin. Silent bots can run
        # without elevated rank.
        if role == 'main':
            ready, rank = await require_bot_admin(client, room)
            if not ready:
                raise RuntimeError(f'bot rank is {rank}; moderator/admin required')

        rec['rank']=rank or rec.get('rank') or 'unknown'
        rec['status']='online'; rec['error']=''; rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))

        if role == 'main':
            # Start listeners BEFORE sending the controller's own greeting.
            # The greeting is only a convenience message; it must never block
            # room/DM command listeners for several seconds after login.
            room_task=asyncio.create_task(child_room_loop(rec),name=f'room-{username}')
            dm_task=asyncio.create_task(child_dm_loop(rec),name=f'dm-{username}')
            try:
                await asyncio.wait_for(child_send_join_greeting(client, room_id), timeout=2.0)
            except asyncio.TimeoutError:
                log.warning('controller greeting timed out; listeners remain active room=%s', room_id)
            except Exception:
                log.exception('controller greeting failed; listeners remain active room=%s', room_id)

        # Keep the session alive. Any transient database/auth/network error
        # escapes to the supervisor, which reconnects the bot automatically.
        while True:
            # Detect a manual kick/removal even when the login/WebSocket session
            # itself is still alive. If membership disappears, rejoin immediately.
            current_rank, rejoined=await ensure_bot_in_room(client,room)
            if current_rank is None:
                # Do not kill the supervisor on the first missed membership read;
                # retry shortly so transient DB latency is not mistaken for a kick.
                rec['status']='reconnecting'
                rec['error']='room membership missing; automatic rejoin in progress'
                rec['updated_at']=now()
                save(BOTS_FILE,load(BOTS_FILE,[]))
                await asyncio.sleep(2)
                continue

            if role == 'main' and current_rank not in BOT_ALLOWED_RANKS:
                # After a kick, room_join may restore the bot first as a normal
                # member. Do NOT leave the room again: keep the session alive
                # and wait for the required controller rank to be restored.
                rec['rank']=current_rank or 'member'
                rec['status']='reconnecting'
                rec['error']='bot is back in the room but is waiting for moderator/admin rank'
                rec['updated_at']=now()
                if rejoined:
                    log.warning('AUTOREJOIN WAITING FOR RANK: @%s room=%s rank=%s',username,room_name,current_rank)
                await asyncio.sleep(3)
                continue

            rec['rank']=current_rank
            hb=await heartbeat_bot(client,room_id)
            if hb is None:
                rec['status']='reconnecting'
                rec['error']='room heartbeat failed; retrying'
                rec['updated_at']=now()
                await asyncio.sleep(2)
                continue
            if rejoined:
                log.info('AUTOREJOIN VERIFIED: @%s room=%s role=%s rank=%s',username,room_name,role,current_rank)
            rec['status']='online'; rec['error']=''; rec['updated_at']=now()
            # Check membership frequently enough to recover quickly after a kick.
            await asyncio.sleep(3)

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        rec['status']='reconnecting'
        rec['error']=str(exc)[:240]
        rec['updated_at']=now()
        save(BOTS_FILE,load(BOTS_FILE,[]))
        log.warning('bot session ended for @%s: %s',username,exc)
    finally:
        for task in (room_task,dm_task):
            if task:
                task.cancel()
        for task in (room_task,dm_task):
            if task:
                try: await task
                except asyncio.CancelledError: pass
                except Exception: pass
        if client is not None:
            # Never call room_leave during an automatic reconnect/rejoin cycle.
            # Explicit removal already calls remove_bot(), which performs the
            # intentional room_leave before cancelling this task.
            try: await asyncio.to_thread(lambda: client.auth.sign_out())
            except Exception: pass
        child_clients.pop(bid,None)


async def child_runner(rec):
    """Persistent supervisor: automatically reconnect every speaking/silent bot."""
    username=rec.get('username','unknown')
    delay=1.0
    while True:
        try:
            await _child_runner_once(rec)
        except asyncio.CancelledError:
            rec['status']='offline'
            rec['updated_at']=now()
            save(BOTS_FILE,load(BOTS_FILE,[]))
            raise
        except Exception as exc:
            rec['status']='reconnecting'
            rec['error']=str(exc)[:240]
            rec['updated_at']=now()
            save(BOTS_FILE,load(BOTS_FILE,[]))
            log.exception('bot supervisor failure for @%s',username)

        # Reconnect with bounded exponential backoff instead of leaving the bot offline.
        rec['status']='reconnecting'
        rec['updated_at']=now()
        save(BOTS_FILE,load(BOTS_FILE,[]))
        await asyncio.sleep(delay)
        delay=min(delay*2.0,30.0)
        # Once a session successfully reaches online, the next failure starts fast again.
        if rec.get('status') == 'online':
            delay=1.0

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
        existing_main=next((b for b in bots
                            if str(b.get('room_id')) == room_key
                            and b.get('role','main') == 'main'), None)
        if existing_main and role == 'main':
            return L('⚠️ يوجد بالفعل بوت أساسي متحكم في الغرفة. لا يمكن إضافة بوت تحكم ثانٍ.', '⚠️ This room already has a Main control bot.')
        # If the exact Main controller is submitted again through hb@, reject it.
        # Other bots may still be added as silent bots to the same room.
        if existing_main and role == 'hang' and str(existing_main.get('username','')).lstrip('@').lower() == str(username).lstrip('@').lower():
            return L(f'🚫 البوت @{str(username).lstrip("@")} هو بوت أساسي متحكم في غرفة «{room.get("name",room_name)}»، ولا يمكن إدخاله كبوت صامت.',
                     f'🚫 @{str(username).lstrip("@")} is already the Main control bot for room «{room.get("name",room_name)}» and cannot be added as a silent bot.')
    # Verify credentials immediately: automatic acceptance only after successful login.
    client,err=await login_client(username,password)
    if err: return L('❌ بيانات البوت غير صحيحة أو تعذر تسجيل الدخول.','❌ Bot credentials are invalid or login failed.')
    try: await join_bot(client,room)
    except Exception: return L('❌ تعذر إدخال البوت إلى الغرفة.','❌ Could not join the room.')
    # Main/control bots must be Moderator/Admin. Silent (hang) bots do not
    # need elevated rank and enter the room immediately after login.
    rank = await member_rank(client, room['id'], client_user_id(client)) or 'unknown'
    if role == 'main':
        ready, rank = await require_bot_admin(client, room)
        if not ready:
            try: await leave_bot(client, room['id'])
            except Exception: pass
            return L(
                f'❌ لم تتم إضافة @{username}. يجب أن يكون بوت التحكم «مشرف» أو «ادمن». الرتبة الحالية: «{rank or "غير معروف"}».',
                f'❌ @{username} was not added. The Main control bot must be Moderator/Admin. Current rank: {rank or "unknown"}.'
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
    has_language=(str(room['id']) in lang_state.get('rooms',{}))
    prompt_sent=False
    if not has_language:
        set_pending_language(master_id, room['id'], room['name'])
        prompt_sent=await send_dm(master_id, language_prompt(room['name']))
        if not prompt_sent:
            log.error('Language prompt could not be delivered to master %s for room %s', master_id, room['name'])
    language_note='\n📩 تم إرسال اختيار اللغة إلى خاصك.' if prompt_sent else ''
    language_note_en='\n📩 Language selection was sent to your DM.' if prompt_sent else ''
    return L(f'✅ تمت إضافة @{username} تلقائياً إلى غرفة {room["name"]}.\n🤖 النوع: {"Main Bot" if role=="main" else "Hang Bot"}'+language_note,f'✅ @{username} was automatically added to {room["name"]}.\n🤖 Type: {"Main Bot" if role=="main" else "Hang Bot"}'+language_note_en)

async def add_silent_bots_batch(usernames, password, room_name, master_id='', master_name=''):
    """Add multiple silent/hang bots using one shared password and room.

    Input is intentionally sequential so each add_bot() sees the latest
    BOTS_FILE/MASTERS_FILE state and concurrent additions cannot overwrite
    each other's records.
    """
    clean=[]
    seen=set()
    for raw in usernames:
        u=str(raw or '').strip().lstrip('@').strip()
        if not u:
            continue
        key=u.casefold()
        if key in seen:
            continue
        seen.add(key)
        clean.append(u)
    if not clean:
        return '❌ لم يتم العثور على أسماء بوتات.'
    if len(clean) > 50:
        return f'❌ الحد الأقصى في الدفعة الواحدة هو 50 بوتاً. أرسلت {len(clean)} بوت.'

    results=[]
    ok_count=0
    for username in clean:
        try:
            result=await add_bot(username, password, room_name, 'hang', str(master_id), master_name)
            results.append((username, result))
            if str(result).startswith('✅'):
                ok_count += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception('batch silent bot add failed for %s', username)
            results.append((username, f'❌ @{username}: تعذر الإضافة ({exc})'))

    lines=[f'🤖 نتيجة إدخال البوتات الصامتة: {ok_count}/{len(clean)}']
    for username, result in results:
        lines.append(str(result))
    return '\n'.join(lines)

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

async def managed_bot_status(rec):
    """Return a live-ish status for a managed bot record."""
    bid=str(rec.get('id',''))
    client=child_clients.get(bid)
    if not client:
        return 'offline'
    try:
        rid=rec.get('room_id')
        uid=client_user_id(client)
        if rid and uid:
            rank=await member_rank(client,rid,uid)
            if rank is None:
                return 'reconnecting'
        task=child_tasks.get(bid)
        if task and task.done():
            return 'offline'
        return 'online'
    except Exception:
        return 'reconnecting'

def _room_grouped_bots():
    bots=load(BOTS_FILE,[])
    groups={}
    for b in bots:
        rid=str(b.get('room_id',''))
        groups.setdefault(rid,[]).append(b)
    return groups

async def _profile_presence(user_id):
    """Determine presence from recent profile heartbeat OR live room membership.

    Some Giant clients can remain present/frozen in a room while their
    profiles.last_seen_at is not refreshed promptly. A current room_members
    row is therefore treated as connected-in-room; this fixes .is for users
    who are visibly still in a room.
    """
    try:
        rows=await asyncio.to_thread(
            lambda: sb.table('profiles').select('last_seen_at').eq('id',str(user_id)).limit(1).execute().data or []
        )
        if rows:
            raw=rows[0].get('last_seen_at')
            if raw:
                value=str(raw).strip().replace('Z','+00:00')
                dt=datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    dt=dt.replace(tzinfo=timezone.utc)
                age=(datetime.now(timezone.utc)-dt).total_seconds()
                if age <= 120:
                    return True
        # Fallback for a user who is still a live member of at least one room.
        members=await asyncio.to_thread(
            lambda: sb.table('room_members').select('room_id').eq('user_id',str(user_id)).limit(1).execute().data or []
        )
        return bool(members)
    except Exception as exc:
        log.warning('presence lookup failed user=%s: %s',user_id,exc)
        return False

async def _user_room_rows(user_id):
    """Read the live room memberships; no stale cache is used."""
    try:
        members=await asyncio.to_thread(
            lambda: sb.table('room_members').select('room_id,rank').eq('user_id',str(user_id)).execute().data or []
        )
        if not members:
            return []
        room_ids=[str(x.get('room_id')) for x in members if x.get('room_id')]
        rooms=await asyncio.to_thread(
            lambda: sb.table('rooms').select('id,name').in_('id',room_ids).execute().data or []
        )
        by_id={str(r.get('id')):r for r in rooms}
        result=[]
        for m in members:
            rid=str(m.get('room_id') or '')
            room=by_id.get(rid)
            if room:
                result.append({'id':rid,'name':str(room.get('name') or rid),'rank':str(m.get('rank') or 'member')})
        result.sort(key=lambda x:x['name'].casefold())
        return result
    except Exception as exc:
        log.warning('user room lookup failed user=%s: %s',user_id,exc)
        return []

async def command_user_info(text):
    """.is username — show whether a user is online and every live room membership."""
    target=str(text or '').strip()
    if target.lower().startswith('.is'):
        target=target[3:].strip()
    target=target.lstrip('@').strip()
    if not target:
        return '❌ استخدم: .is اسم_المستخدم'
    profile,err=await resolve_profile(target)
    if err or not profile:
        return f'🔎 المستخدم @{target} غير موجود.'
    uid=str(profile.get('id'))
    username=str(profile.get('username') or target)
    online=await _profile_presence(uid)
    rooms=await _user_room_rows(uid)
    lines=[f'🔎 المستخدم: @{username}', f'📡 الحالة: {"🟢 متصل" if online else "🔴 غير متصل"}']
    if rooms:
        lines.append('🏠 الغرف:')
        for room in rooms:
            lines.append(f'• {room["name"]} — {room["rank"]}')
    else:
        lines.append('🏠 الغرف: لا يوجد حالياً')
    return '\n'.join(lines)

async def command_room_members(client, room_id, sender_id, next_page=False):
    """.r/.nx: live room users, 20 usernames per page, no @ and no ranks."""
    try:
        rows=await asyncio.to_thread(
            lambda: client.table('room_members').select('user_id').eq('room_id',room_id).execute().data or []
        )
    except Exception as exc:
        log.warning('room member list failed room=%s: %s',room_id,exc)
        return '❌ تعذر جلب قائمة أعضاء الغرفة حالياً.'
    if not rows:
        ROOM_MEMBER_PAGES.pop(f'{room_id}:{sender_id}',None)
        return '👥 لا يوجد مستخدمون مسجلون حالياً في الغرفة.'

    ids=list(dict.fromkeys(str(x.get('user_id')) for x in rows if x.get('user_id')))
    names=[]
    if ids:
        try:
            profiles=await asyncio.to_thread(
                lambda: sb.table('profiles').select('id,username').in_('id',ids).execute().data or []
            )
            by_id={str(x.get('id')):str(x.get('username') or '').strip().lstrip('@') for x in profiles}
            names=[by_id.get(uid,uid) for uid in ids if by_id.get(uid,uid)]
        except Exception:
            names=ids

    names=sorted(dict.fromkeys(names), key=lambda x:x.casefold())
    key=f'{room_id}:{sender_id}'
    if next_page:
        page=ROOM_MEMBER_PAGES.get(key,0)+1
    else:
        page=0
    total=len(names)
    pages=max(1,(total+19)//20)
    if page>=pages:
        return f'📄 لا توجد صفحة تالية. آخر صفحة هي {pages}/{pages}.'
    ROOM_MEMBER_PAGES[key]=page
    chunk=names[page*20:(page+1)*20]
    lines=[f'👥 مستخدمو الغرفة — {total}', f'📄 الصفحة {page+1}/{pages}']
    lines.extend(f'• {name}' for name in chunk)
    if pages>1:
        lines.append('➡️ أرسل .nx للقائمة التالية.')
    return '\n'.join(lines)

async def command_bot_info(sender,text):
    """.gis username — legacy managed-bot status command."""
    target=str(text or '').strip()
    if target.lower().startswith('.gis'):
        target=target[4:].strip()
    target=target.lstrip('@').strip()
    if not target:
        return '❌ استخدم: .gis اسم_البوت'
    bots=load(BOTS_FILE,[])
    matches=[b for b in bots if str(b.get('username','')).lstrip('@').casefold()==target.casefold()]
    if not matches:
        return f'🔎 لا يوجد بوت مسجل باسم @{target}.'
    lines=[f'🔎 حالة البوت @{target}:']
    for b in matches:
        status=await managed_bot_status(b)
        role='متحكم' if b.get('role')=='main' else 'صامت'
        state='🟢 أون لاين' if status=='online' else f'🟡 {status}'
        lines.append(f'• 🏠 {b.get("room_name") or b.get("room_id") or "غير معروف"} — {role} — {state}')
    return '\n'.join(lines)

async def command_rooms_list(sender):
    """.gr — legacy list of managed rooms and their bots."""
    bots=load(BOTS_FILE,[])
    masters=load(MASTERS_FILE,{})
    if not bots:
        return '📋 لا توجد غرف أو بوتات مسجلة حالياً.'
    groups=_room_grouped_bots()
    lines=['📋 قائمة الغرف والبوتات:']
    for rid,items in groups.items():
        m=masters.get(rid,{})
        master=m.get('master_name') or 'غير محدد'
        room_name=m.get('room_name') or items[0].get('room_name') or rid
        lines.append(f'\n🏠 {room_name} | الماستر الأساسي: @{master}')
        for b in items:
            status=await managed_bot_status(b)
            role='متحكم' if b.get('role')=='main' else 'صامت'
            state='🟢 أون لاين' if status=='online' else f'🟡 {status}'
            lines.append(f'  • @{b.get("username","?")} — {role} — {state}')
    return '\n'.join(lines)

async def control_action(sender,text):
    t=text.strip(); low=t.lower()

    # Language selection is handled before help so the selected language is
    # applied immediately to the next help request.
    pending=pending_language(sender)
    if pending:
        choice=low.strip()
        if choice in ('1','ar','arabic','عربي','العربية'):
            value='ar'
        elif choice in ('2','en','english','انجليزي','الانجليزية','الإنجليزية'):
            value='en'
        else:
            return '🌐 اختر اللغة أولاً بإرسال:\n1️⃣ العربية\n2️⃣ English\n\nثم أرسل help لعرض الخيارات.'
        room_id=str(pending.get('room_id'))
        set_room_lang(room_id,value)
        set_user_lang(sender,value)
        pop_pending_language(sender)
        # If the Main bot is already connected, send its one-time room greeting
        # now, using the language just selected.
        for rec in load(BOTS_FILE,[]):
            if str(rec.get('room_id'))==room_id and rec.get('role','main')=='main':
                client=child_clients.get(rec.get('id'))
                if client:
                    await child_send_join_greeting(client,room_id)
                break
        return ('✅ تم اختيار العربية لبوت التحكم في هذه الغرفة.\n📌 أرسل help لعرض جميع الخيارات.' if value=='ar'
                else '✅ English selected for the control bot in this room.\n📌 Send help to view all options.')

    # Global user lookup works from the control-bot DM. Legacy managed-bot
    # commands are kept under .gis and .gr.
    if low == '.is' or low.startswith('.is '):
        return await command_user_info(t)
    if low == '.gis' or low.startswith('.gis '):
        return await command_bot_info(sender,t)
    if low == '.gr':
        return await command_rooms_list(sender)
    if low == '.r':
        return '📌 الأمر .r يعمل من داخل الغرفة لعرض جميع المستخدمين الموجودين فيها.'

    if low in ('help','مساعدة','الاوامر','الأوامر'):
        return L(
            '🛠️ أوامر سيرفر التحكم\n'
            '1️⃣ user@pass@room — إنشاء البوت المتحكم\n'
            '2️⃣ hb@user@pass@room — إنشاء بوت صامت\n'
            '   hb@bot1 bot2 bot3@pass@room — إدخال عدة بوتات صامتة دفعة واحدة\n'
            '3️⃣ del@room — حذف البوت المتحكم من الغرفة\n'
            '4️⃣ delall@room — حذف جميع البوتات من الغرفة\n'
            '5️⃣ clean@room — تنظيف ذاكرة الغرفة\n'
            '6️⃣ bots — عرض البوتات الفعالة\n'
            '7️⃣ room@name — معلومات الغرفة\n'
            '8️⃣ lang@ar / lang@en — تغيير اللغة\n'
            '9️⃣ master@user@room — إضافة ماستر\n'
            '🔟 delmaster@user@room — إزالة ماستر\n'
            '1️⃣1️⃣ masters@room — عرض ماسترات الغرفة\n'
            '1️⃣2️⃣ .is اسم_المستخدم — حالة المستخدم والغرف الموجودة فيها\n'
            '1️⃣3️⃣ .r — داخل الغرفة: عرض جميع المستخدمين\n'
            '1️⃣4️⃣ .gis اسم_البوت — حالة البوت (الأمر السابق)\n'
            '1️⃣5️⃣ .gr — عرض جميع الغرف والبوتات (الأمر السابق)\n\n'
            '⚠️ أول شخص يضيف البوت بنجاح يصبح الماستر الأساسي بعد التحقق من رتبة البوت.',
            '🛠️ Control Server Commands\n'
            '1️⃣ user@pass@room — Create Main control bot\n'
            '2️⃣ hb@user@pass@room — Create silent bot\n'
            '   hb@bot1 bot2 bot3@pass@room — Add multiple silent bots in one batch\n'
            '3️⃣ del@room — Delete the Main control bot\n'
            '4️⃣ delall@room — Delete all bots from the room\n'
            '5️⃣ clean@room — Clean room control memory\n'
            '6️⃣ bots — Show active bots\n'
            '7️⃣ room@name — Show room information\n'
            '8️⃣ lang@ar / lang@en — Change language\n'
            '9️⃣ master@user@room — Add a master\n'
            '🔟 delmaster@user@room — Remove a master\n'
            '1️⃣1️⃣ masters@room — List room masters\n'
            '1️⃣2️⃣ .is username — Show user online status and rooms\n'
            '1️⃣3️⃣ .r — Inside a room: list all current users\n'
            '1️⃣4️⃣ .gis username — Managed-bot status (legacy command)\n'
            '1️⃣5️⃣ .gr — List all managed rooms and bots (legacy command)\n\n'
            '⚠️ The first user who successfully adds the bot becomes the primary master after the bot rank is verified.',
            user_id=sender)

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
    excluded=('room@','del@','delall@','clean@','lang@','لغة@','hb@','بوتصامت@','صامت@','master@','delmaster@','masters@')
    if '@' in t and not low.startswith(excluded):
        payload, room = t.rsplit('@', 1)
        if '@' in payload:
            username, password = payload.split('@', 1)
            username=username.strip().lstrip('@'); password=password.strip(); room=room.strip()
            if username and password and room:
                return await add_bot(username,password,room,'main',str(sender),sender_name)
    # Silent bot aliases: بوتصامت@user@password@room / صامت@user@password@room
    if low.startswith('بوتصامت@') or low.startswith('صامت@'):
        prefix='بوتصامت@' if low.startswith('بوتصامت@') else 'صامت@'
        payload=t[len(prefix):]
        if '@' in payload:
            payload, room = payload.rsplit('@', 1)
            if '@' in payload:
                username, password = payload.split('@', 1)
                if username.strip() and password.strip() and room.strip():
                    return await add_bot(username.strip().lstrip('@'),password.strip(),room.strip(),'hang',str(sender),sender_name)
        return 'استخدم: hb@اسم_البوت@كلمة_المرور@اسم_الغرفة'

    if low.startswith('hb@'):
        payload=t[3:].strip()
        if '@' in payload:
            payload, room = payload.rsplit('@', 1)
            payload=payload.strip(); room=room.strip()
            if '@' in payload and room:
                # Batch syntax: hb@bot1 bot2 bot3@password@room
                # A shared password is used for every listed silent bot.
                users_part, password = payload.rsplit('@', 1)
                users=[x.strip() for x in users_part.replace('،', ' ').replace(',', ' ').split() if x.strip()]
                password=password.strip()
                # Preserve the old single-bot syntax where the password itself
                # may contain '@': hb@user@gag@998877@room
                if len(users) == 1 and not any(ch.isspace() for ch in users_part) and ',' not in users_part and '،' not in users_part:
                    username, single_password = payload.split('@', 1)
                    username=username.strip().lstrip('@')
                    single_password=single_password.strip()
                    if username and single_password:
                        return await add_bot(username,single_password,room,'hang',str(sender),sender_name)
                if users and password:
                    if len(users) > 1:
                        return await add_silent_bots_batch(users,password,room,str(sender),sender_name)
                    return await add_bot(users[0].lstrip('@'),password,room,'hang',str(sender),sender_name)
        return 'استخدم: hb@اسم_البوت@كلمة_المرور@اسم_الغرفة\nأو: hb@bot1 bot2 bot3@كلمة_المرور@اسم_الغرفة'
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

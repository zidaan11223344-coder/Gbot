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

SERVER_URL=os.environ.get('SUPABASE_URL','').strip()
SERVER_KEY=os.environ.get('SUPABASE_KEY','').strip()
CONTROL_USERNAME=os.environ.get('GIANT_USERNAME','').strip()
CONTROL_PASSWORD=os.environ.get('GIANT_PASSWORD','')
DEFAULT_LANG=(os.environ.get('CONTROL_LANGUAGE') or 'ar').strip().lower()
ROOM_PASSWORD=os.environ.get('ROOM_PASSWORD','')
POLL=float(os.environ.get('CONTROL_POLL_SECONDS','2'))
if not SERVER_URL or not SERVER_KEY or not CONTROL_USERNAME or not CONTROL_PASSWORD:
    raise SystemExit('Missing SUPABASE_URL/SUPABASE_KEY/GIANT_USERNAME/GIANT_PASSWORD')

def create_supabase_client(url, key):
    # Match the working bot: support modern sb_publishable_* keys.
    if str(key).startswith("sb_publishable_"):
        client = create_client(url, "a.b.c")
        client.supabase_key = key
        try:
            headers = client.options.headers
            headers["apikey"] = key
            # Publishable keys are API keys, not JWT bearer tokens.
            headers["Authorization"] = f"Bearer {key}"
        except Exception:
            pass
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
    """Resolve Giant username to the internal Giant Auth email mapping."""
    username = str(username or "").strip()
    if not username:
        return ""

    normalized = re.sub(r"[^a-z0-9_]", "", username.lower())
    default_email = f"{normalized}@giant.app" if normalized else ""

    # The app's deterministic username -> auth email mapping is the primary path.
    # It avoids requiring table/RPC read access from a publishable key.
    if default_email:
        return default_email

    try:
        d = await asyncio.to_thread(
            lambda: client.rpc("lookup_auth_email", {"_username": username}).execute().data
        )
        if isinstance(d, str) and "@" in d:
            return d.strip()
    except Exception:
        pass

    try:
        rows = await asyncio.to_thread(
            lambda: client.table("profiles").select("auth_email")
            .eq("username", username).limit(1).execute().data or []
        )
        if rows and rows[0].get("auth_email"):
            return str(rows[0]["auth_email"]).strip()
    except Exception:
        pass

    return default_email

async def login_client(username,password):
    client=create_client(SERVER_URL,SERVER_KEY)
    email=await resolve_email(client,username)
    res=await run(lambda: client.auth.sign_in_with_password({'email':email,'password':password}))
    if not res or not getattr(res,'user',None): return None, 'login failed'
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

async def require_bot_admin(client, room):
    uid = getattr(getattr(client, 'auth', None), 'user', None)
    uid = getattr(uid, 'id', None)
    if not uid:
        return False, 'bot user id unavailable'
    rank = await member_rank(client, room['id'], uid)
    if rank not in BOT_ALLOWED_RANKS:
        return False, rank or 'not_member'
    return True, rank

async def announce(client, room_id, text):
    # Used only for connection status; feature bots remain independent.
    try:
        uid=getattr(getattr(client,'auth',None),'user',None)
        uid=getattr(uid,'id',None)
        if uid:
            await asyncio.to_thread(lambda: client.table('room_messages').insert({'room_id':room_id,'user_id':uid,'content':text,'message_type':'text'}).execute())
    except Exception: pass

async def child_runner(rec):
    bid=rec['id']; username=rec['username']; password=rec['password']; role=rec.get('role','main'); room_id=rec.get('room_id'); room_name=rec.get('room_name','')
    client,err=await login_client(username,password)
    if err:
        rec['status']='error'; rec['error']=err; save(BOTS_FILE, load(BOTS_FILE,[])); return
    child_clients[bid]=client
    rec['status']='online'; rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))
    try:
        room={'id':room_id,'name':room_name}
        await join_bot(client,room)
        ready, rank = await require_bot_admin(client, room)
        if not ready:
            rec['status']='error'
            rec['error']=f'bot rank is {rank}; moderator/admin required'
            save(BOTS_FILE,load(BOTS_FILE,[]))
            try: await leave_bot(client,room_id)
            except Exception: pass
            return
        rec['rank']=rank; rec['status']='online'; rec['updated_at']=now(); save(BOTS_FILE,load(BOTS_FILE,[]))
        while True:
            # If the room removes the bot's moderation rank, stop managing it.
            current_rank = await member_rank(client, room_id, getattr(getattr(client,'auth',None),'user',None).id)
            if current_rank not in BOT_ALLOWED_RANKS:
                rec['status']='error'; rec['error']=f'bot rank changed to {current_rank or "unknown"}; moderator/admin required'
                save(BOTS_FILE,load(BOTS_FILE,[]))
                break
            await heartbeat_bot(client,room_id)
            await asyncio.sleep(15)
    except asyncio.CancelledError: pass
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
    # Only the person who creates the first successful bot for a room gets
    # the initial language prompt; that person is the primary room master.
    if is_first_bot:
        set_pending_language(master_id, room['id'], room['name'])
        await send_dm(master_id, language_prompt(room['name']))
    return L(f'✅ تمت إضافة @{username} تلقائياً إلى غرفة {room["name"]}.\n🤖 النوع: {"Main Bot" if role=="main" else "Hang Bot"}'+('\n📩 تم إرسال اختيار اللغة إلى خاصك.' if is_first_bot else ''),f'✅ @{username} was automatically added to {room["name"]}.\n🤖 Type: {"Main Bot" if role=="main" else "Hang Bot"}'+('\n📩 Language selection was sent to your DM.' if is_first_bot else ''))

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
    if '@' in t and not low.startswith(('room@','del@','delall@','clean@','lang@','لغة@','hb@','master@','delmaster@','masters@')):
        parts=t.split('@')
        if len(parts)>=3:
            room=parts[-1].strip(); password=parts[-2].strip(); username='@'.join(parts[:-2]).strip().lstrip('@')
            if username and password and room: return await add_bot(username,password,room,'main',str(sender),sender_name)
    if low.startswith('hb@'):
        parts=t.split('@',3)
        if len(parts)==4:
            _,username,password,room=parts
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
    email=await resolve_email(sb,CONTROL_USERNAME)
    res=await run(lambda: sb.auth.sign_in_with_password({'email':email,'password':CONTROL_PASSWORD}))
    if not res or not getattr(res,'user',None): raise RuntimeError('Control bot login failed')
    BOT_ID=res.user.id; log.info('Control bot connected as @%s',CONTROL_USERNAME)
    # Restart persisted bots automatically.
    for b in load(BOTS_FILE,[]):
        try:
            task=asyncio.create_task(child_runner(b),name=f'bot-{b.get("username")}'); child_tasks[b['id']]=task
        except Exception: log.exception('child start failed')
    await dm_loop()

if __name__=='__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: pass

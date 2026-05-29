import discord
from discord.ext import commands
import yt_dlp
import asyncio
import os
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ============ إعداد اللوقز ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
log = logging.getLogger('musicbot')

# ============ الإنتنتس ============
intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(command_prefix='#', intents=intents, help_command=None)

# ============ MongoDB ============
MONGO_URI = os.environ.get('MONGO_URI')
mongo_client = None
playlists_col = None

try:
    if MONGO_URI:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command('ping')
        db = mongo_client['musicbot']
        playlists_col = db['playlists']
        log.info('✅ اتصال MongoDB ناجح')
    else:
        log.warning('⚠️ MONGO_URI غير موجود — أوامر القوائم معطّلة')
except PyMongoError as e:
    log.error(f'❌ فشل الاتصال بـ MongoDB: {e}')
    playlists_col = None

# ============ إعدادات FFmpeg و yt-dlp ============
FFMPEG_OPTIONS = {
    'before_options': (
        '-reconnect 1 -reconnect_streamed 1 '
        '-reconnect_delay_max 5 -nostdin'
    ),
    'options': '-vn -filter:a "volume=0.5"'
}

YDL_OPTIONS = {
    'format': 'bestaudio/best',
    'noplaylist': True,
    'quiet': True,
    'no_warnings': True,
    'default_search': 'scsearch',
    'socket_timeout': 10,
    'retries': 3,
}

# ============ حالة كل سيرفر ============
queues = {}
locks = {}


def get_queue(guild_id: int):
    if guild_id not in queues:
        queues[guild_id] = []
    return queues[guild_id]


def get_lock(guild_id: int) -> asyncio.Lock:
    if guild_id not in locks:
        locks[guild_id] = asyncio.Lock()
    return locks[guild_id]


def cleanup_guild(guild_id: int):
    queues.pop(guild_id, None)
    locks.pop(guild_id, None)


def is_supported_url(query: str) -> bool:
    q = query.lower()
    return 'soundcloud.com' in q or 'snd.sc' in q


def is_blocked_url(query: str) -> bool:
    q = query.lower()
    return 'youtube.com' in q or 'youtu.be' in q


# ============ استخراج المعلومات بشكل غير متزامن ============
async def extract_info(query: str):
    loop = asyncio.get_event_loop()

    def _extract():
        with yt_dlp.YoutubeDL(YDL_OPTIONS) as ydl:
            if is_supported_url(query):
                info = ydl.extract_info(query, download=False)
            else:
                info = ydl.extract_info(f"scsearch:{query}", download=False)
                if 'entries' in info and info['entries']:
                    info = info['entries'][0]
                elif 'entries' in info:
                    return None
            return {
                'url': info['url'],
                'title': info.get('title', 'بدون اسم'),
            }

    return await loop.run_in_executor(None, _extract)


# ============ تشغيل التالي ============
async def play_next(ctx):
    if not ctx.voice_client or not ctx.voice_client.is_connected():
        return

    queue = get_queue(ctx.guild.id)
    if not queue:
        try:
            await ctx.send('✅ انتهت القائمة!')
        except discord.HTTPException:
            pass
        return

    url, title = queue.pop(0)
    try:
        source = discord.FFmpegPCMAudio(url, **FFMPEG_OPTIONS)
        ctx.voice_client.play(
            source,
            after=lambda e: _after_play(ctx, e)
        )
        await ctx.send(f'🎵 **يشغل الآن:** {title}')
    except Exception as e:
        log.error(f'فشل تشغيل {title}: {e}')
        await ctx.send(f'⚠️ ما قدرت أشغل **{title}** — أتخطاها')
        await play_next(ctx)


def _after_play(ctx, error):
    if error:
        log.error(f'خطأ بعد التشغيل: {error}')
    fut = asyncio.run_coroutine_threadsafe(play_next(ctx), bot.loop)
    try:
        fut.result(timeout=30)
    except Exception as e:
        log.error(f'خطأ في play_next: {e}')


# ============ أحداث ============
@bot.event
async def on_ready():
    log.info(f'✅ البوت شغال: {bot.user}')
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.listening,
            name='#اوامر'
        )
    )


@bot.event
async def on_voice_state_update(member, before, after):
    if member == bot.user:
        return
    vc = member.guild.voice_client
    if vc and vc.channel and len(vc.channel.members) == 1:
        await asyncio.sleep(60)
        if vc.channel and len(vc.channel.members) == 1:
            await vc.disconnect()
            cleanup_guild(member.guild.id)


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f'❌ ينقص شي من الأمر! اكتب `#اوامر` للمساعدة')
    elif isinstance(error, commands.CommandNotFound):
        return
    else:
        log.error(f'خطأ في الأمر: {error}')
        try:
            await ctx.send('❌ صار خطأ! حاول مرة ثانية')
        except discord.HTTPException:
            pass


# ============ أوامر التشغيل ============
async def ensure_voice(ctx) -> bool:
    if not ctx.author.voice:
        await ctx.send('❌ لازم تكون في روم صوتي!')
        return False
    try:
        if not ctx.voice_client:
            await ctx.author.voice.channel.connect(timeout=10)
        elif ctx.voice_client.channel != ctx.author.voice.channel:
            await ctx.voice_client.move_to(ctx.author.voice.channel)
        return True
    except asyncio.TimeoutError:
        await ctx.send('❌ ما قدرت أدخل الروم!')
        return False


@bot.command(name='شغل', aliases=['غني'])
async def play(ctx, *, query: str):
    if is_blocked_url(query):
        return await ctx.send(
            '⚠️ روابط يوتيوب غير مدعومة. استخدم رابط ساوند كلاود '
            'أو اكتب اسم الأغنية للبحث.'
        )

    if not await ensure_voice(ctx):
        return

    async with ctx.typing():
        info = await extract_info(query)
        if not info:
            return await ctx.send('❌ ما لقيت هذي الأغنية! جرب اسم ثاني')

    if ctx.voice_client.is_playing() or ctx.voice_client.is_paused():
        get_queue(ctx.guild.id).append((info['url'], info['title']))
        await ctx.send(f'➕ **أضيف للقائمة:** {info["title"]}')
    else:
        get_queue(ctx.guild.id).insert(0, (info['url'], info['title']))
        await play_next(ctx)


@bot.command(name='تخطى', aliases=['سكب'])
async def skip(ctx):
    if ctx.voice_client and (ctx.voice_client.is_playing() or ctx.voice_client.is_paused()):
        ctx.voice_client.stop()
        await ctx.send('⏭️ تم التخطي!')
    else:
        await ctx.send('❌ ما في شي يشتغل!')


@bot.command(name='قائمة')
async def queue_cmd(ctx):
    queue = get_queue(ctx.guild.id)
    if not queue:
        return await ctx.send('📭 القائمة فارغة!')
    msg = '**📋 قائمة الانتظار:**\n'
    for i, (_, title) in enumerate(queue[:15], 1):
        msg += f'`{i}.` {title}\n'
    if len(queue) > 15:
        msg += f'\n_...و {len(queue) - 15} أغنية ثانية_'
    await ctx.send(msg)


@bot.command(name='وقف')
async def stop(ctx):
    if ctx.voice_client:
        queues[ctx.guild.id] = []
        ctx.voice_client.stop()
        await ctx.send('⏹️ وقف التشغيل وتم مسح القائمة')
    else:
        await ctx.send('❌ البوت مو في روم!')


@bot.command(name='مسح')
async def clear_queue(ctx):
    queue = get_queue(ctx.guild.id)
    if queue:
        count = len(queue)
        queue.clear()
        await ctx.send(f'🗑️ تم مسح {count} أغنية والحالية تكمل!')
    else:
        await ctx.send('📭 القائمة فارغة أصلاً!')


@bot.command(name='اخرج')
async def leave(ctx):
    if ctx.voice_client:
        await ctx.voice_client.disconnect()
        cleanup_guild(ctx.guild.id)
        await ctx.send('👋 خرجت!')
    else:
        await ctx.send('❌ البوت مو في روم!')


@bot.command(name='توقف')
async def pause(ctx):
    if ctx.voice_client and ctx.voice_client.is_playing():
        ctx.voice_client.pause()
        await ctx.send('⏸️ تم الإيقاف المؤقت')
    else:
        await ctx.send('❌ ما في شي يشتغل!')


@bot.command(name='كمل')
async def resume(ctx):
    if ctx.voice_client and ctx.voice_client.is_paused():
        ctx.voice_client.resume()
        await ctx.send('▶️ استمر التشغيل')
    else:
        await ctx.send('❌ ما في شي متوقف!')


# ============ أوامر القوائم المحفوظة ============
def check_db(ctx):
    if playlists_col is None:
        asyncio.create_task(ctx.send('❌ قاعدة البيانات غير متاحة حاليًا'))
        return False
    return True


@bot.command(name='حفظ')
async def save_song(ctx, playlist_name: str, *, song: str):
    if not check_db(ctx):
        return
    if is_blocked_url(song):
        return await ctx.send('⚠️ روابط يوتيوب غير مدعومة')
    try:
        playlists_col.update_one(
            {'guild_id': str(ctx.guild.id), 'name': playlist_name},
            {'$push': {'songs': song}},
            upsert=True
        )
        await ctx.send(f'✅ تم حفظ **{song}** في قائمة **{playlist_name}**')
    except PyMongoError as e:
        log.error(f'فشل الحفظ: {e}')
        await ctx.send('❌ صار خطأ في الحفظ')


@bot.command(name='شغل_قائمة')
async def play_playlist(ctx, playlist_name: str):
    if not check_db(ctx):
        return

    lock = get_lock(ctx.guild.id)
    if lock.locked():
        return await ctx.send('⏳ في قائمة تتحمل حاليًا، استنى شوي!')

    async with lock:
        try:
            data = playlists_col.find_one(
                {'guild_id': str(ctx.guild.id), 'name': playlist_name}
            )
        except PyMongoError:
            return await ctx.send('❌ صار خطأ في قاعدة البيانات')

        if not data or not data.get('songs'):
            return await ctx.send(f'❌ ما لقيت قائمة باسم **{playlist_name}**')

        if not await ensure_voice(ctx):
            return

        songs = data['songs']
        status = await ctx.send(
            f'📋 جاري تحميل قائمة **{playlist_name}** ({len(songs)} أغنية)...'
        )

        results = await asyncio.gather(
            *[extract_info(s) for s in songs],
            return_exceptions=True
        )

        loaded = 0
        for r in results:
            if isinstance(r, dict) and r:
                get_queue(ctx.guild.id).append((r['url'], r['title']))
                loaded += 1

        await status.edit(
            content=f'✅ تم تحميل {loaded} من {len(songs)} أغنية من **{playlist_name}**'
        )

        if loaded == 0:
            return

        if not ctx.voice_client.is_playing() and not ctx.voice_client.is_paused():
            await play_next(ctx)


@bot.command(name='عرض_قائمة')
async def show_playlist(ctx, playlist_name: str):
    if not check_db(ctx):
        return
    try:
        data = playlists_col.find_one(
            {'guild_id': str(ctx.guild.id), 'name': playlist_name}
        )
    except PyMongoError:
        return await ctx.send('❌ صار خطأ في قاعدة البيانات')

    if not data or not data.get('songs'):
        return await ctx.send(f'❌ ما لقيت قائمة باسم **{playlist_name}**')

    songs = data['songs']
    msg = f'**📋 قائمة {playlist_name}** ({len(songs)} أغنية):\n'
    for i, song in enumerate(songs[:20], 1):
        msg += f'`{i}.` {song}\n'
    if len(songs) > 20:
        msg += f'\n_...و {len(songs) - 20} أغنية ثانية_'
    await ctx.send(msg)


@bot.command(name='قوائمي')
async def my_playlists(ctx):
    if not check_db(ctx):
        return
    try:
        data = list(playlists_col.find({'guild_id': str(ctx.guild.id)}))
    except PyMongoError:
        return await ctx.send('❌ صار خطأ في قاعدة البيانات')

    if not data:
        return await ctx.send('📭 ما عندك أي قوائم محفوظة!')

    msg = '**📋 قوائمك المحفوظة:**\n'
    for p in data:
        msg += f'• **{p["name"]}** — {len(p.get("songs", []))} أغنية\n'
    await ctx.send(msg)


@bot.command(name='حذف_قائمة')
async def delete_playlist(ctx, playlist_name: str):
    if not check_db(ctx):
        return
    try:
        result = playlists_col.delete_one(
            {'guild_id': str(ctx.guild.id), 'name': playlist_name}
        )
    except PyMongoError:
        return await ctx.send('❌ صار خطأ في قاعدة البيانات')

    if result.deleted_count:
        await ctx.send(f'🗑️ تم حذف قائمة **{playlist_name}**')
    else:
        await ctx.send(f'❌ ما لقيت قائمة باسم **{playlist_name}**')


@bot.command(name='حذف_اغنية')
async def remove_song(ctx, playlist_name: str, index: int):
    if not check_db(ctx):
        return
    try:
        data = playlists_col.find_one(
            {'guild_id': str(ctx.guild.id), 'name': playlist_name}
        )
    except PyMongoError:
        return await ctx.send('❌ صار خطأ في قاعدة البيانات')

    if not data or not data.get('songs'):
        return await ctx.send(f'❌ ما لقيت قائمة باسم **{playlist_name}**')

    songs = data['songs']
    if index < 1 or index > len(songs):
        return await ctx.send(f'❌ الرقم لازم يكون بين 1 و {len(songs)}')

    removed = songs.pop(index - 1)
    playlists_col.update_one(
        {'guild_id': str(ctx.guild.id), 'name': playlist_name},
        {'$set': {'songs': songs}}
    )
    await ctx.send(f'🗑️ تم حذف **{removed}** من **{playlist_name}**')


# ============ قائمة الأوامر ============
@bot.command(name='اوامر')
async def commands_list(ctx):
    msg = """
🎵 **أوامر المغني جود:**

**التشغيل:**
`#شغل` [اسم أو رابط ساوند كلاود] — شغّل أغنية
`#تخطى` — تخطى الحالية
`#توقف` — إيقاف مؤقت
`#كمل` — كمّل التشغيل
`#وقف` — وقف ومسح كل شي
`#مسح` — امسح القائمة فقط
`#اخرج` — أخرج البوت
`#قائمة` — اعرض قائمة الانتظار

**القوائم المحفوظة:**
`#حفظ` [اسم_القائمة] [أغنية] — احفظ أغنية
`#شغل_قائمة` [اسم] — شغّل قائمة كاملة
`#عرض_قائمة` [اسم] — اعرض محتوى القائمة
`#قوائمي` — اعرض كل قوائمك
`#حذف_اغنية` [اسم] [رقم] — احذف أغنية محددة
`#حذف_قائمة` [اسم] — احذف قائمة كاملة

⚠️ _البوت يدعم ساوند كلاود فقط_
"""
    await ctx.send(msg)


# ============ التشغيل ============
TOKEN = os.environ.get('DISCORD_TOKEN')
if not TOKEN:
    log.error('❌ DISCORD_TOKEN غير موجود في متغيرات البيئة!')
    raise SystemExit(1)

bot.run(TOKEN, log_handler=None)

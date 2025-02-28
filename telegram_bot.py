from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorageRecord
from aiogram.utils import executor
import asyncio
from futures_bot import FuturesBot  # Import the bot class
import pandas as pd
from datetime import datetime

# Telegram Bot Setup
BOT_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'  # Replace with your BotFather token
CHAT_ID = 'YOUR_CHAT_ID'  # Replace with your Telegram chat ID (get from @userinfobot)
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorageRecord()
dp = Dispatcher(bot, storage=storage)

# Global bot instance
bot_instance = FuturesBot()

async def send_messages():
    while True:
        if bot_instance.message_queue:
            message = bot_instance.message_queue.pop(0)
            await bot.send_message(CHAT_ID, message)
        await asyncio.sleep(1)

@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    if not bot_instance.running:
        bot_instance.running = True
        asyncio.create_task(bot_instance.run())
        await message.reply("Bot started!")
    else:
        await message.reply("Bot is already running!")

@dp.message_handler(commands=['stop'])
async def stop_command(message: types.Message):
    if bot_instance.running:
        bot_instance.running = False
        await message.reply("Bot stopped!")
    else:
        await message.reply("Bot is already stopped!")

@dp.message_handler(commands=['status'])
async def status_command(message: types.Message):
    status = (f"Bot Status\n"
              f"Running: {bot_instance.running}\n"
              f"Bankroll: {bot_instance.bankroll - bot_instance.daily_loss:.2f} USDT\n"
              f"Daily Loss: {bot_instance.daily_loss:.2f} USDT\n"
              f"Open Position: {bool(bot_instance.position)}\n"
              f"Trades Today: {len([t for t in bot_instance.trades if pd.to_datetime(t['entry_time']).date() == datetime.now().date()])}")
    await message.reply(status)

if __name__ == "__main__":
    asyncio.ensure_future(send_messages())
    executor.start_polling(dp, skip_updates=True)
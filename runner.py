import asyncio
import subprocess

async def run_streamer():
    process = await asyncio.create_subprocess_exec(
        'python3', 'stream_kline.py',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        print(f"Streamer: {line.decode().strip()}")
    await process.wait()

async def run_futures_bot():
    process = await asyncio.create_subprocess_exec(
        'python3', 'futures_bot.py',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        print(f"FuturesBot: {line.decode().strip()}")
    await process.wait()

async def run_telegram_bot():
    process = await asyncio.create_subprocess_exec(
        'python3', 'telegram_bot.py',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        print(f"TelegramBot: {line.decode().strip()}")
    await process.wait()

async def main():
    tasks = [
        run_streamer(),
        run_futures_bot(),
        run_telegram_bot()
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
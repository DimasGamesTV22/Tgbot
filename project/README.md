# PC Repair Telegram Bot

A Telegram bot for handling PC repair orders.

## Setup

1. Create a new bot on Telegram:
   - Message @BotFather on Telegram
   - Use the /newbot command
   - Follow the instructions to create your bot
   - Copy the bot token provided

2. Configure the bot:
   - Add your bot token to the .env file:
     ```
     TELEGRAM_BOT_TOKEN=your_bot_token_here
     ```

3. Install dependencies:
   ```bash
   npm install
   ```

4. Start the bot:
   ```bash
   npm run dev
   ```

## Features

- Submit repair requests
- Check repair status
- Cancel repair requests

## Commands

- /start - Start the bot
- /repair - Submit a new repair request
- /status - Check repair status
- /cancel - Cancel repair request
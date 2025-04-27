import TelegramBot from 'node-telegram-bot-api';
import moment from 'moment';
import logger from './logger';
import dotenv from 'dotenv';
import schedule from 'node-schedule';
import NodeCache from 'node-cache';
import { format } from 'date-fns';
import axios from 'axios';
import sanitizeHtml from 'sanitize-html';
import validator from 'validator';

dotenv.config();

// Initialize bot with token from environment variables
const bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: true });

// Initialize cache for rate limiting and temporary data
const cache = new NodeCache({ stdTTL: 3600 });

// Initialize collections
const repairRequests = new Map();
const userPoints = new Map();
const rateLimits = new Map();
const userSettings = new Map();
const notifications = new Map();

// Initialize repair services
const repairServices = [
  { id: 'service_1', name: 'Диагностика ПК', price: 1500, description: 'Полная диагностика компьютера', duration: '1-2 часа' },
  { id: 'service_2', name: 'Чистка от пыли', price: 2000, description: 'Профессиональная чистка компьютера от пыли', duration: '1-2 часа' },
  { id: 'service_3', name: 'Замена термопасты', price: 1000, description: 'Замена термопасты на процессоре', duration: '30-60 минут' },
  { id: 'service_4', name: 'Установка Windows', price: 2500, description: 'Установка операционной системы Windows', duration: '2-3 часа' },
  { id: 'service_5', name: 'Замена HDD/SSD', price: 1500, description: 'Замена жесткого диска или SSD', duration: '1-2 часа' },
  { id: 'service_6', name: 'Сборка ПК', price: 5000, description: 'Профессиональная сборка компьютера', duration: '2-4 часа' }
];

// Initialize special offers
const specialOffers = [
  { id: 'offer_1', name: 'Комплексная диагностика', price: 3000, points: 450, description: 'Полная диагностика + чистка от пыли', duration: '2-3 часа' },
  { id: 'offer_2', name: 'Базовое обслуживание', price: 2500, points: 250, description: 'Чистка от пыли + замена термопасты', duration: '2-3 часа' },
  { id: 'offer_3', name: 'Максимальный пакет', price: 7000, points: 750, description: 'Диагностика + чистка + Windows + термопаста', duration: '4-6 часов' }
];

// Initialize help topics
const helpTopics = {
  general: {
    title: 'Общая информация',
    content: 'Добро пожаловать в PC Repair Bot!\n\n' +
      'Этот бот поможет вам:\n' +
      '• Оформить заявку на ремонт компьютера\n' +
      '• Узнать статус вашей заявки\n' +
      '• Получить информацию о наших услугах\n' +
      '• Участвовать в программе лояльности\n' +
      '• Получать уведомления о статусе заказа\n' +
      '• Управлять настройками профиля'
  },
  services: {
    title: 'Услуги',
    content: 'Мы предоставляем следующие услуги:\n\n' +
      '• Диагностика компьютера\n' +
      '• Чистка от пыли\n' +
      '• Замена термопасты\n' +
      '• Установка Windows\n' +
      '• Замена комплектующих\n' +
      '• Сборка компьютера\n\n' +
      'Каждая услуга выполняется опытными специалистами с использованием профессионального оборудования.'
  },
  loyalty: {
    title: 'Программа лояльности',
    content: 'За каждый заказ вы получаете бонусные баллы:\n\n' +
      '• Стандартный ремонт: 100-500 баллов\n' +
      '• Специальные предложения: до 750 баллов\n\n' +
      'Баллы можно использовать для получения скидок на будущие заказы.\n' +
      'Накопленные баллы действительны в течение 12 месяцев.'
  },
  contact: {
    title: 'Контакты',
    content: 'Наши контакты:\n\n' +
      '📞 Телефон: +7 (XXX) XXX-XX-XX\n' +
      '📧 Email: support@pcrepair.com\n' +
      '📍 Адрес: г. Москва, ул. Примерная, д. 1\n\n' +
      'Режим работы:\n' +
      'Пн-Пт: 9:00 - 20:00\n' +
      'Сб-Вс: 10:00 - 18:00'
  },
  creator: {
    title: 'О создателе',
    content: 'Бот разработан @dmitryilife\n' +
      'По вопросам разработки и сотрудничества обращайтесь в личные сообщения.\n\n' +
      'Дополнительные контакты:\n' +
      '• GitHub: github.com/dmitryilife\n' +
      '• LinkedIn: linkedin.com/in/dmitryilife\n' +
      '• Email: dmitry@example.com'
  }
};

// Helper functions
const isAdmin = (userId) => {
  const adminIds = process.env.ADMIN_IDS.split(',').map(Number);
  return adminIds.includes(userId);
};

const rateLimit = (userId) => {
  const now = Date.now();
  const limit = rateLimits.get(userId);
  
  if (!limit || now - limit > 2000) {
    rateLimits.set(userId, now);
    return true;
  }
  return false;
};

const getUserPoints = (userId) => {
  return userPoints.get(userId) || 0;
};

const addUserPoints = (userId, points, reason) => {
  const currentPoints = getUserPoints(userId);
  const newTotal = currentPoints + points;
  userPoints.set(userId, newTotal);
  
  logger.info('Loyalty points added', {
    userId,
    pointsAdded: points,
    reason,
    newTotal
  });
  
  return newTotal;
};

const scheduleReminder = (userId, requestId, time) => {
  const job = schedule.scheduleJob(time, async () => {
    try {
      const request = repairRequests.get(requestId);
      if (request && request.status === 'pending') {
        await bot.sendMessage(
          userId,
          `⏰ Напоминание: у вас есть активная заявка #${requestId}\n` +
          'Не забудьте подтвердить удобное время для ремонта!'
        );
      }
    } catch (error) {
      logger.error('Error sending reminder', { error, userId, requestId });
    }
  });
  
  notifications.set(requestId, job);
};

const validatePhoneNumber = (phone) => {
  return validator.isMobilePhone(phone, 'ru-RU');
};

const sanitizeInput = (input) => {
  return sanitizeHtml(input, {
    allowedTags: [],
    allowedAttributes: {}
  });
};

// Keyboard generators
const getMainKeyboard = (isAdmin) => {
  const keyboard = {
    reply_markup: {
      keyboard: [
        ['🔧 Заказать ремонт', '📱 Мой профиль'],
        ['🎁 Специальные предложения', '❓ Помощь'],
        ['⚙️ Настройки', '📞 Контакты']
      ],
      resize_keyboard: true
    }
  };

  if (isAdmin) {
    keyboard.reply_markup.keyboard.push(['⚙️ Панель администратора']);
  }

  return keyboard;
};

const getAdminKeyboard = () => ({
  reply_markup: {
    keyboard: [
      ['📋 Активные заявки', '📊 Все заявки'],
      ['👥 Управление клиентами', '📈 Статистика'],
      ['📢 Рассылка', '⚙️ Настройки бота'],
      ['🏠 Главное меню']
    ],
    resize_keyboard: true
  }
});

const getServicesKeyboard = () => ({
  reply_markup: {
    inline_keyboard: repairServices.map(service => ([{
      text: `${service.name} - ${service.price}₽`,
      callback_data: service.id
    }]))
  }
});

const getSpecialOffersKeyboard = () => ({
  reply_markup: {
    inline_keyboard: [
      ...specialOffers.map(offer => ([{
        text: `${offer.name} - ${offer.price}₽ (${offer.points} баллов)`,
        callback_data: offer.id
      }])),
      [{ text: '🔙 Назад', callback_data: 'back_to_main' }]
    ]
  }
});

const getHelpKeyboard = () => ({
  reply_markup: {
    inline_keyboard: [
      [{ text: 'Общая информация', callback_data: 'help_general' }],
      [{ text: 'Услуги', callback_data: 'help_services' }],
      [{ text: 'Программа лояльности', callback_data: 'help_loyalty' }],
      [{ text: 'Контакты', callback_data: 'help_contact' }],
      [{ text: 'О создателе', callback_data: 'help_creator' }],
      [{ text: '🔙 Назад', callback_data: 'back_to_main' }]
    ]
  }
});

const getSettingsKeyboard = () => ({
  reply_markup: {
    inline_keyboard: [
      [{ text: '🔔 Уведомления', callback_data: 'settings_notifications' }],
      [{ text: '📱 Контактные данные', callback_data: 'settings_contacts' }],
      [{ text: '🌍 Язык', callback_data: 'settings_language' }],
      [{ text: '🔙 Назад', callback_data: 'back_to_main' }]
    ]
  }
});

// Command handlers
bot.onText(/\/start/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const username = msg.from.username;
  const adminStatus = isAdmin(userId);

  logger.info('Start command received', {
    userId,
    username,
    isAdmin: adminStatus
  });

  const welcomeMessage = adminStatus
    ? '👋 Добро пожаловать в панель администратора!'
    : '👋 Добро пожаловать в PC Repair Bot!\n\n' +
      'Здесь вы можете:\n' +
      '• Заказать ремонт компьютера\n' +
      '• Узнать статус заказа\n' +
      '• Получить бонусные баллы\n' +
      '• Воспользоваться специальными предложениями\n' +
      '• Настроить уведомления\n' +
      '• Получить техническую поддержку';

  const keyboard = adminStatus ? getAdminKeyboard() : getMainKeyboard(adminStatus);
  
  await bot.sendMessage(chatId, welcomeMessage, keyboard);
});

// Settings handler
bot.onText(/⚙️ Настройки/, async (msg) => {
  const chatId = msg.chat.id;
  
  if (!rateLimit(chatId)) {
    await bot.sendMessage(chatId, '⚠️ Пожалуйста, подождите немного перед следующим запросом.');
    return;
  }

  const settings = userSettings.get(chatId) || {
    notifications: true,
    language: 'ru',
    phone: '',
    email: ''
  };

  let message = '⚙️ *Настройки*\n\n' +
    `🔔 Уведомления: ${settings.notifications ? 'Включены' : 'Выключены'}\n` +
    `🌍 Язык: ${settings.language === 'ru' ? 'Русский' : 'English'}\n` +
    `📱 Телефон: ${settings.phone || 'Не указан'}\n` +
    `📧 Email: ${settings.email || 'Не указан'}`;

  await bot.sendMessage(chatId, message, {
    parse_mode: 'Markdown',
    ...getSettingsKeyboard()
  });
});

// Service handlers
bot.onText(/🔧 Заказать ремонт/, async (msg) => {
  const chatId = msg.chat.id;
  
  if (!rateLimit(chatId)) {
    await bot.sendMessage(chatId, '⚠️ Пожалуйста, подождите немного перед следующим запросом.');
    return;
  }

  await bot.sendMessage(
    chatId,
    '🔧 Выберите услугу из списка:\n\n' +
    repairServices.map(service =>
      `*${service.name}*\n` +
      `💰 Цена: ${service.price}₽\n` +
      `⏱ Длительность: ${service.duration}\n` +
      `📝 ${service.description}\n`
    ).join('\n'),
    {
      parse_mode: 'Markdown',
      ...getServicesKeyboard()
    }
  );
});

// Special offers handler
bot.onText(/🎁 Специальные предложения/, async (msg) => {
  const chatId = msg.chat.id;
  
  if (!rateLimit(chatId)) {
    await bot.sendMessage(chatId, '⚠️ Пожалуйста, подождите немного перед следующим запросом.');
    return;
  }

  await bot.sendMessage(
    chatId,
    '🎁 Специальные предложения:\n\n' +
    specialOffers.map(offer =>
      `*${offer.name}*\n` +
      `💰 Цена: ${offer.price}₽\n` +
      `⭐ Бонус: ${offer.points} баллов\n` +
      `⏱ Длительность: ${offer.duration}\n` +
      `📝 ${offer.description}\n`
    ).join('\n'),
    {
      parse_mode: 'Markdown',
      ...getSpecialOffersKeyboard()
    }
  );
});

// Profile handler
bot.onText(/📱 Мой профиль/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  
  if (!rateLimit(chatId)) {
    await bot.sendMessage(chatId, '⚠️ Пожалуйста, подождите немного перед следующим запросом.');
    return;
  }

  const userRequests = Array.from(repairRequests.values())
    .filter(req => req.userId === userId)
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

  const points = getUserPoints(userId);
  const totalSpent = userRequests.reduce((sum, req) => sum + req.finalPrice, 0);
  const settings = userSettings.get(userId) || {};

  let message = '📱 *Ваш профиль:*\n\n' +
    `👤 ID: ${userId}\n` +
    `📞 Телефон: ${settings.phone || 'Не указан'}\n` +
    `📧 Email: ${settings.email || 'Не указан'}\n` +
    `⭐ Баллы: ${points}\n` +
    `💰 Потрачено: ${totalSpent}₽\n` +
    `📊 Всего заказов: ${userRequests.length}\n\n`;

  if (userRequests.length > 0) {
    message += '*Последние заказы:*\n\n';
    
    for (const request of userRequests.slice(0, 3)) {
      const service = request.isSpecialOffer
        ? specialOffers.find(o => o.id === request.serviceId)
        : repairServices.find(s => s.id === request.serviceId);

      message += `🔧 ${service ? service.name : 'Услуга'}\n` +
        `💰 Стоимость: ${request.finalPrice}₽\n` +
        `📅 Дата: ${moment(request.createdAt).format('DD.MM.YYYY HH:mm')}\n` +
        `📋 Статус: ${getStatusEmoji(request.status)}\n\n`;
    }
  }

  const keyboard = {
    inline_keyboard: [
      [{ text: '📋 История заказов', callback_data: 'show_history' }],
      [{ text: '⚙️ Настройки профиля', callback_data: 'settings_profile' }],
      [{ text: '🔙 Назад', callback_data: 'back_to_main' }]
    ]
  };

  await bot.sendMessage(chatId, message, {
    parse_mode: 'Markdown',
    reply_markup: keyboard
  });
});

// Help handler
bot.onText(/❓ Помощь/, async (msg) => {
  const chatId = msg.chat.id;
  
  if (!rateLimit(chatId)) {
    await bot.sendMessage(chatId, '⚠️ Пожалуйста, подождите немного перед следующим запросом.');
    return;
  }

  await bot.sendMessage(
    chatId,
    '❓ Выберите раздел справки:',
    getHelpKeyboard()
  );
});

// Admin panel handler
bot.onText(/⚙️ Панель администратора/, async (msg) => {
  const chatId = msg.chat.id;
  
  if (!isAdmin(chatId)) {
    await bot.sendMessage(chatId, '⛔ У вас нет доступа к панели администратора');
    return;
  }

  if (!rateLimit(chatId)) {
    await bot.sendMessage(chatId, '⚠️ Превышен лимит запросов. Пожалуйста, подождите.');
    return;
  }

  await bot.sendMessage(
    chatId,
    '⚙️ Панель администратора',
    getAdminKeyboard()
  );
});

// Broadcast message handler
bot.onText(/📢 Рассылка/, async (msg) => {
  const chatId = msg.chat.id;
  
  if (!isAdmin(chatId)) {
    await bot.sendMessage(chatId, '⛔ У вас нет доступа к этой функции');
    return;
  }

  cache.set(`${chatId}_state`, 'awaiting_broadcast');
  
  await bot.sendMessage(
    chatId,
    '📢 Введите текст сообщения для рассылки:',
    {
      reply_markup: {
        force_reply: true
      }
    }
  );
});

// Active requests handler
bot.onText(/📋 Активные заявки/, async (msg) => {
  const chatId = msg.chat.id;
  
  try {
    if (!isAdmin(chatId)) {
      await bot.sendMessage(chatId, '⛔ У вас нет доступа к этой функции');
      return;
    }

    if (!rateLimit(chatId)) {
      await bot.sendMessage(chatId, '⚠️ Превышен лимит запросов. Пожалуйста, подождите.');
      return;
    }

    const activeRequests = Array.from(repairRequests.values())
      .filter(req => ['pending', 'in_progress'].includes(req.status))
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    if (activeRequests.length === 0) {
      await bot.sendMessage(chatId, '📋 Активных заявок нет', {
        reply_markup: {
          inline_keyboard: [[{ text: '🔙 Назад', callback_data: 'back_to_admin' }]]
        }
      });
      return;
    }

    const statusEmoji = {
      pending: '⏳',
      in_progress: '🔧'
    };

    let message = '📋 *Активные заявки:*\n\n';

    for (const request of activeRequests) {
      const service = request.isSpecialOffer 
        ? specialOffers.find(o => o.id === request.serviceId)
        : repairServices.find(s => s.id === request.serviceId);

      const user = await bot.getChatMember(request.userId, request.userId)
        .catch(() => ({ user: { first_name: 'Неизвестно' } }));

      const settings = userSettings.get(request.userId) || {};

      message += `${statusEmoji[request.status]} Заявка #${request.id}\n` +
        `👤 Клиент: ${user.user.first_name}\n` +
        `📱 ID: ${request.userId}\n` +
        `📞 Телефон: ${settings.phone || 'Не указан'}\n` +
        `📧 Email: ${settings.email || 'Не указан'}\n` +
        `📝 ${service ? service.name : 'Услуга'}\n` +
        `💰 Стоимость: ${request.finalPrice}₽\n` +
        `📅 Создана: ${moment(request.createdAt).format('DD.MM.YYYY HH:mm')}\n\n`;
    }

    const keyboard = activeRequests.map(req => ([{
      text: `✏️ Заявка #${req.id}`,
      callback_data: `update_${req.id}`
    }]));

    keyboard.push([{ text: '🔙 Назад', callback_data: 'back_to_admin' }]);

    await bot.sendMessage(chatId, message, {
      parse_mode: 'Markdown',
      reply_markup: {
        inline_keyboard: keyboard
      }
    });
  } catch (error) {
    logger.error('Error showing active requests', { error, chatId });
    await bot.sendMessage(chatId, 'Произошла ошибка при загрузке активных заявок.');
  }
});

// All requests handler
bot.onText(/📊 Все заявки/, async (msg) => {
  const chatId = msg.chat.id;
  
  try {
    if (!isAdmin(chatId)) {
      await bot.sendMessage(chatId, '⛔ У вас нет доступа к этой функции');
      return;
    }

    if (!rateLimit(chatId)) {
      await bot.sendMessage(chatId, '⚠️ Превышен лимит запросов. Пожалуйста, подождите.');
      return;
    }

    const allRequests = Array.from(repairRequests.values())
      .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    if (allRequests.length === 0) {
      await bot.sendMessage(chatId, '📋 Заявок пока нет', {
        reply_markup: {
          inline_keyboard: [[{ text: '🔙 Назад', callback_data: 'back_to_admin' }]]
        }
      });
      return;
    }

    const statusEmoji = {
      pending: '⏳',
      in_progress: '🔧',
      completed: '✅',
      cancelled: '❌'
    };

    let message = '📊 *Все заявки:*\n\n';
    let currentDate = '';

    for (const request of allRequests) {
      const requestDate = moment(request.createdAt).format('DD.MM.YYYY');
      
      if (requestDate !== currentDate) {
        currentDate = requestDate;
        message += `📅 *${currentDate}*\n\n`;
      }

      const service = request.isSpecialOffer 
        ? specialOffers.find(o => o.id === request.serviceId)
        : repairServices.find(s => s.id === request.serviceId);

      const settings = userSettings.get(request.userId) || {};

      message += `${statusEmoji[request.status]} Заявка #${request.id}\n` +
        `📱 ID клиента: ${request.userId}\n` +
        `📞 Телефон: ${settings.phone || 'Не указан'}\n` +
        `📧 Email: ${settings.email || 'Не указан'}\n` +
        `📝 ${service ? service.name : 'Услуга'}\n` +
        `💰 Стоимость: ${request.finalPrice}₽\n` +
        `⏰ Время: ${moment(request.createdAt).format('HH:mm')}\n\n`;
    }

    // Split message if it's too long
    const maxLength = 4096;
    if (message.length > maxLength) {
      const parts = message.match(new RegExp(`.{1,${maxLength}}`, 'g'));
      for (const part of parts) {
        await bot.sendMessage(chatId, part, {
          parse_mode: 'Markdown'
        });
      }
    } else {
      await bot.sendMessage(chatId, message, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [[{ text: '🔙 Назад', callback_data: 'back_to_admin' }]]
        }
      });
    }
  } catch (error) {
    logger.error('Error showing all requests', { error, chatId });
    await bot.sendMessage(chatId, 'Произошла ошибка при загрузке всех заявок.');
  }
});

// Client management handler
bot.onText(/👥 Управление клиентами/, async (msg) => {
  const chatId = msg.chat.id;
  
  try {
    if (!isAdmin(chatId)) {
      await bot.sendMessage(chatId, '⛔ У вас нет доступа к этой функции');
      return;
    }

    if (!rateLimit(chatId)) {
      await bot.sendMessage(chatId, '⚠️ Превышен лимит запросов. Пожалуйста, подождите.');
      return;
    }

    // Get unique clients from requests
    const clients = new Map();
    
    for (const request of repairRequests.values()) {
      if (!clients.has(request.userId)) {
        const userRequests = Array.from(repairRequests.values())
          .filter(req => req.userId === request.userId);
        
        const totalSpent = userRequests.reduce((sum, req) => sum + req.finalPrice, 0);
        const points = getUserPoints(request.userId);
        const settings = userSettings.get(request.userId) || {};
        
        clients.set(request.userId, {
          id: request.userId,
          username: request.userName,
          phone: settings.phone,
          email: settings.email,
          requestCount: userRequests.length,
          totalSpent,
          points,
          lastActive: Math.max(...userRequests.map(req => new Date(req.createdAt)))
        });
      }
    }

    const sortedClients = Array.from(clients.values())
      .sort((a, b) => b.totalSpent - a.totalSpent);

    let message = '👥 *Список клиентов:*\n\n';

    for (const client of sortedClients) {
      message += `👤 Клиент: ${client.username || 'Без имени'}\n` +
        `📱 ID: ${client.id}\n` +
        `📞 Телефон: ${client.phone || 'Не указан'}\n` +
        `📧 Email: ${client.email || 'Не указан'}\n` +
        `📊 Заявок: ${client.requestCount}\n` +
        `💰 Потрачено: ${client.totalSpent}₽\n` +
        `⭐ Баллы: ${client.points}\n` +
        `📅 Последняя активность: ${moment(client.lastActive).format('DD.MM.YYYY HH:mm')}\n\n`;
    }

    // Split message if it's too long
    const maxLength = 4096;
    if (message.length > maxLength) {
      const parts = message.match(new RegExp(`.{1,${maxLength}}`, 'g'));
      for (const part of parts) {
        await bot.sendMessage(chatId, part, {
          parse_mode: 'Markdown'
        });
      }
    } else {
      await bot.sendMessage(chatId, message, {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [{ text: '📊 Экспорт данных', callback_data: 'export_clients' }],
            [{ text: '🔙 Назад', callback_data: 'back_to_admin' }]
          ]
        }
      });
    }
  } catch (error) {
    logger.error('Error showing clients', { error, chatId });
    await bot.sendMessage(chatId, 'Произошла ошибка при загрузке списка клиентов.');
  }
});

// Statistics handler
bot.onText(/📈 Статистика/, async (msg) => {
  const chatId = msg.chat.id;
  
  try {
    if (!isAdmin(chatId)) {
      await bot.sendMessage(chatId, '⛔ У вас нет доступа к этой функции');
      return;
    }

    if (!rateLimit(chatId)) {
      await bot.sendMessage(chatId, '⚠️ Превышен лимит запросов. Пожалуйста, подождите.');
      return;
    }

    const allRequests = Array.from(repairRequests.values());
    const today = moment().startOf('day');
    const thisWeek = moment().startOf('week');
    const thisMonth = moment().startOf('month');
    
    // Calculate statistics
    const stats = {
      total: allRequests.length,
      active: allRequests.filter(r => ['pending', 'in_progress'].includes(r.status)).length,
      completed: allRequests.filter(r => r.status === 'completed').length,
      cancelled: allRequests.filter(r => r.status === 'cancelled').length,
      totalRevenue: allRequests.reduce((sum, req) => sum + req.finalPrice, 0),
      uniqueClients: new Set(allRequests.map(r => r.userId)).size,
      today: {
        requests: allRequests.filter(r => moment(r.createdAt).isAfter(today)).length,
        revenue: allRequests
          .filter(r => moment(r.createdAt).isAfter(today))
          .reduce((sum, req) => sum + req.finalPrice, 0)
      },
      week: {
        requests: allRequests.filter(r => moment(r.createdAt).isAfter(thisWeek)).length,
        revenue: allRequests
          .filter(r => moment(r.createdAt).isAfter(thisWeek))
          .reduce((sum, req) => sum + req.finalPrice, 0)
      },
      month: {
        requests: allRequests.filter(r => moment(r.createdAt).isAfter(thisMonth)).length,
        revenue: allRequests
          .filter(r => moment(r.createdAt).isAfter(thisMonth))
          .reduce((sum, req) => sum + req.finalPrice, 0)
      }
    };

    // Calculate service popularity
    const serviceStats = new Map();
    allRequests.forEach(request => {
      const serviceId = request.serviceId;
      const current = serviceStats.get(serviceId) || 0;
      serviceStats.set(serviceId, current + 1);
    });

    const popularServices = Array.from(serviceStats.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([id, count]) => {
        const service = repairServices.find(s => s.id === id) || 
          specialOffers.find(o => o.id === id);
        return {
          name: service ? service.name : 'Неизвестная услуга',
          count,
          percentage: Math.round((count / allRequests.length) * 100)
        };
      });

    let message = '📈 *Общая статистика:*\n\n' +
      `📊 Всего заявок: ${stats.total}\n` +
      `⏳ Активных: ${stats.active}\n` +
      `✅ Завершено: ${stats.completed}\n` +
      `❌ Отменено: ${stats.cancelled}\n` +
      `💰 Общая выручка: ${stats.totalRevenue}₽\n` +
      `👥 Уникальных клиентов: ${stats.uniqueClients}\n\n` +
      '*Статистика за период:*\n\n' +
      `📅 Сегодня:\n` +
      `• Заявок: ${stats.today.requests}\n` +
      `• Выручка: ${stats.today.revenue}₽\n\n` +
      `📅 Эта неделя:\n` +
      `• Заявок: ${stats.week.requests}\n` +
      `• Выручка: ${stats.week.revenue}₽\n\n` +
      `📅 Этот месяц:\n` +
      `• Заявок: ${stats.month.requests}\n` +
      `• Выручка: ${stats.month.revenue}₽\n\n` +
      `🏆 *Топ 5 популярных услуг:*\n\n`;

    popularServices.forEach((service, index) => {
      message += `${index + 1}. ${service.name}\n` +
        `• Заказов: ${service.count}\n` +
        `• Доля: ${service.percentage}%\n\n`;
    });

    const keyboard = {
      inline_keyboard: [
        [
          { text: '📊 Подробный отчёт', callback_data: 'detailed_stats' },
          { text: '📥 Экспорт данных', callback_data: 'export_stats' }
        ],
        [{ text: '🔙 Назад', callback_data: 'back_to_admin' }]
      ]
    };

    await bot.sendMessage(chatId, message, {
      parse_mode: 'Markdown',
      reply_markup: keyboard
    });
  } catch (error) {
    logger.error('Error showing statistics', { error, chatId });
    await bot.sendMessage(chatId, 'Произошла ошибка при загрузке статистики.');
  }
});

// Return to main menu handler
bot.onText(/🏠 Главное меню/, async (msg) => {
  const chatId = msg.chat.id;
  bot.emit('text', { text: '/start', chat: { id: chatId } });
});

// Callback query handler
bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const userId = query.from.id;
  const action = query.data;

  logger.info('Callback query received', {
    userId,
    action
  });

  try {
    if (action.startsWith('help_')) {
      const topicKey = action.replace('help_', '');
      const topic = helpTopics[topicKey];

      if (topic) {
        await bot.editMessageText(
          `*${topic.title}*\n\n${topic.content}`,
          {
            chat_id: chatId,
            message_id: query.message.message_id,
            parse_mode: 'Markdown',
            reply_markup: getHelpKeyboard().reply_markup
          }
        );
      }
    } else if (action === 'back_to_main') {
      await bot.deleteMessage(chatId, query.message.message_id);
      bot.emit('text', { text: '/start', chat: { id: chatId } });
    } else if (action === 'back_to_admin') {
      await bot.deleteMessage(chatId, query.message.message_id);
      bot.emit('text', { text: '⚙️ Панель администратора', chat: { id: chatId } });
    } else if (action.startsWith('service_')) {
      const service = repairServices.find(s => s.id === action);
      if (service) {
        const requestId = Date.now();
        repairRequests.set(requestId, {
          id: requestId,
          userId,
          serviceId: service.id,
          status: 'pending',
          finalPrice: service.price,
          createdAt: new Date(),
          isSpecialOffer: false
        });

        addUserPoints(userId, Math.floor(service.price / 10), 'New repair request');

        // Schedule a reminder for 24 hours
        const reminderTime = new Date();
        reminderTime.setHours(reminderTime.getHours() + 24);
        scheduleReminder(userId, requestId, reminderTime);

        await bot.sendMessage(
          chatId,
          `✅ Заявка на "${service.name}" успешно создана!\n\n` +
          `🔢 Номер заявки: #${requestId}\n` +
          `💰 Стоимость: ${service.price}₽\n` +
          `⏱ Примерная длительность: ${service.duration}\n\n` +
          `⭐ Вам начислено ${Math.floor(service.price / 10)} бонусных баллов!\n\n` +
          'Мы свяжемся с вами для уточнения деталей.\n' +
          'Напоминание будет отправлено через 24 часа, если статус не изменится.',
          {
            reply_markup: {
              inline_keyboard: [[{ text: '🔙 Назад', callback_data: 'back_to_main' }]]
            }
          }
        );

        // Notify admins about new request
        const adminIds = process.env.ADMIN_IDS.split(',');
        for (const adminId of adminIds) {
          await bot.sendMessage(
            adminId,
            `🆕 Новая заявка #${requestId}\n\n` +
            `👤 Клиент: ${userId}\n` +
            `📝 Услуга: ${service.name}\n` +
            `💰 Стоимость: ${service.price}₽\n` +
            `📅 Создана: ${moment().format('DD.MM.YYYY HH:mm')}`
          );
        }
      }
    } else if (action.startsWith('offer_')) {
      const offer = specialOffers.find(o => o.id === action);
      if (offer) {
        const requestId = Date.now();
        repairRequests.set(requestId, {
          id: requestId,
          userId,
          serviceId: offer.id,
          status: 'pending',
          finalPrice: offer.price,
          createdAt: new Date(),
          isSpecialOffer: true
        });

        addUserPoints(userId, offer.points, 'Special offer purchase');

        // Schedule a reminder for 24 hours
        const reminderTime = new Date();
        reminderTime.setHours(reminderTime.getHours() + 24);
        scheduleReminder(userId, requestId, reminderTime);

        await bot.sendMessage(
          chatId,
          `✅ Заказ "${offer.name}" успешно оформлен!\n\n` +
          `🔢 Номер заказа: #${requestId}\n` +
          `💰 Стоимость: ${offer.price}₽\n` +
          `⏱ Примерная длительность: ${offer.duration}\n\n` +
          `⭐ Вам начислено ${offer.points} бонусных баллов!\n\n` +
          'Мы свяжемся с вами для уточнения деталей.\n' +
          'Напоминание будет отправлено через 24 часа, если статус не изменится.',
          {
            reply_markup: {
              inline_keyboard: [[{ text: '🔙 Назад', callback_data: 'back_to_main' }]]
            }
          }
        );

        // Notify admins about new request
        const adminIds = process.env.ADMIN_IDS.split(',');
        for (const adminId of adminIds) {
          await bot.sendMessage(
            adminId,
            `🆕 Новая заявка #${requestId} (Спецпредложение)\n\n` +
            `👤 Клиент: ${userId}\n` +
            `📝 Услуга: ${offer.name}\n` +
            `💰 Стоимость: ${offer.price}₽\n` +
            `📅 Создана: ${moment().format('DD.MM.YYYY HH:mm')}`
          );
        }
      }
    } else if (action.startsWith('update_')) {
      const requestId = parseInt(action.replace('update_', ''));
      const request = repairRequests.get(requestId);

      if (request && isAdmin(userId)) {
        const service = request.isSpecialOffer
          ? specialOffers.find(o => o.id === request.serviceId)
          : repairServices.find(s => s.id === request.serviceId);

        const settings = userSettings.get(request.userId) || {};

        const statusKeyboard = {
          reply_markup: {
            inline_keyboard: [
              [
                { text: '⏳ В ожидании', callback_data: `status_${requestId}_pending` },
                { text: '🔧 В работе', callback_data: `status_${requestId}_in_progress` }
              ],
              [
                { text: '✅ Завершено', callback_data: `status_${requestId}_completed` },
                { text: '❌ Отменено', callback_data: `status_${requestId}_cancelled` }
              ],
              [
                { text: '📝 Добавить комментарий', callback_data: `comment_${requestId}` },
                { text: '📅 Назначить время', callback_data: `schedule_${requestId}` }
              ],
              [{ text: '🔙 Назад', callback_data: 'back_to_admin' }]
            ]
          }
        };

        await bot.editMessageText(
          `*Заявка #${requestId}*\n\n` +
          `👤 Клиент ID: ${request.userId}\n` +
          `📞 Телефон: ${settings.phone || 'Не указан'}\n` +
          `📧 Email: ${settings.email || 'Не указан'}\n` +
          `📝 Услуга: ${service ? service.name : 'Неизвестная услуга'}\n` +
          `💰 Стоимость: ${request.finalPrice}₽\n` +
          `📅 Создана: ${moment(request.createdAt).format('DD.MM.YYYY HH:mm')}\n` +
          `📋 Статус: ${getStatusEmoji(request.status)}\n` +
          (request.comment ? `💬 Комментарий: ${request.comment}\n` : '') +
          (request.scheduledTime ? `⏰ Назначено время: ${moment(request.scheduledTime).format('DD.MM.YYYY HH:mm')}\n` : ''),
          {
            chat_id: chatId,
            message_id: query.message.message_id,
            parse_mode: 'Markdown',
            ...statusKeyboard
          }
        );
      }
    } else if (action.startsWith('status_')) {
      const [, requestId, newStatus] = action.split('_');
      const request = repairRequests.get(parseInt(requestId));

      if (request && isAdmin(userId)) {
        request.status = newStatus;
        await bot.answerCallbackQuery(query.id, { text: '✅ Статус обновлен' });
        
        // Cancel reminder if exists
        const reminder = notifications.get(parseInt(requestId));
        if (reminder) {
          reminder.cancel();
          notifications.delete(parseInt(requestId));
        }

        // Notify client about status change
        const statusMessages = {
          pending: '⏳ Ваша заявка ожидает обработки',
          in_progress: '🔧 Ваша заявка взята в работу',
          completed: '✅ Ваша заявка выполнена',
          cancelled: '❌ Ваша заявка отменена'
        };

        const service = request.isSpecialOffer
          ? specialOffers.find(o => o.id === request.serviceId)
          : repairServices.find(s => s.id === request.serviceId);

        await bot.sendMessage(
          request.userId,
          `📢 Обновление статуса заявки #${requestId}\n\n` +
          `📝 Услуга: ${service ? service.name : 'Услуга'}\n` +
          statusMessages[newStatus] +
          (newStatus === 'completed' ? '\n\nСпасибо за доверие! Будем рады видеть вас снова!' : '')
        );

        // Return to active requests list
        bot.emit('text', { text: '📋 Активные заявки', chat: { id: chatId } });
      }
    } else if (action.startsWith('comment_')) {
      const requestId = parseInt(action.replace('comment_', ''));
      
      if (isAdmin(userId)) {
        cache.set(`${chatId}_state`, `awaiting_comment_${requestId}`);
        
        await bot.sendMessage(
          chatId,
          '💬 Введите комментарий к заявке:',
          {
            reply_markup: {
              force_reply: true
            }
          }
        );
      }
    } else if (action.startsWith('schedule_')) {
      const requestId = parseInt(action.replace('schedule_', ''));
      
      if (isAdmin(userId)) {
        cache.set(`${chatId}_state`, `awaiting_schedule_${requestId}`);
        
        await bot.sendMessage(
          chatId,
          '📅 Введите дату и время в формате DD.MM.YYYY HH:mm:',
          {
            reply_markup: {
              force_reply: true
            }
          }
        );
      }
    } else if (action === 'export_stats') {
      if (isAdmin(userId)) {
        const allRequests = Array.from(repairRequests.values());
        
        // Generate CSV data
        let csv = 'ID,User ID,Service,Price,Status,Created At\n';
        
        for (const request of allRequests) {
          const service = request.isSpecialOffer
            ? specialOffers.find(o => o.id === request.serviceId)
            : repairServices.find(s => s.id === request.serviceId);
            
          csv += `${request.id},${request.userId},"${service ? service.name : 'Unknown'}",${request.finalPrice},${request.status},"${moment(request.createdAt).format('YYYY-MM-DD HH:mm:ss')}"\n`;
        }
        
        // Create temporary file
        const filename = `statistics_${moment().format('YYYY-MM-DD_HH-mm')}.csv`;
        
        await bot.sendDocument(chatId, Buffer.from(csv), {
          filename,
          caption: '📊 Статистика экспортирована в CSV'
        });
      }
    } else if (action === 'export_clients') {
      if (isAdmin(userId)) {
        const clients = new Map();
        
        for (const request of repairRequests.values()) {
          if (!clients.has(request.userId)) {
            const userRequests = Array.from(repairRequests.values())
              .filter(req => req.userId === request.userId);
            
            const settings = userSettings.get(request.userId) || {};
            
            clients.set(request.userId, {
              id: request.userId,
              phone: settings.phone || '',
              email: settings.email || '',
              totalOrders: userRequests.length,
              totalSpent: userRequests.reduce((sum, req) => sum + req.finalPrice, 0),
              points: getUserPoints(request.userId),
              lastActive: Math.max(...userRequests.map(req => new Date(req.createdAt)))
            });
          }
        }
        
        // Generate CSV data
        let csv = 'User ID,Phone,Email,Total Orders,Total Spent,Points,Last Active\n';
        
        for (const client of clients.values()) {
          csv += `${client.id},"${client.phone}","${client.email}",${client.totalOrders},${client.totalSpent},${client.points},"${moment(client.lastActive).format('YYYY-MM-DD HH:mm:ss')}"\n`;
        }
        
        // Create temporary file
        const filename = `clients_${moment().format('YYYY-MM-DD_HH-mm')}.csv`;
        
        await bot.sendDocument(chatId, Buffer.from(csv), {
          filename,
          caption: '👥 Данные клиентов экспортированы в CSV'
        });
      }
    } else if (action.startsWith('settings_')) {
      const setting = action.replace('settings_', '');
      const settings = userSettings.get(userId) || {};
      
      switch (setting) {
        case 'notifications':
          settings.notifications = !settings.notifications;
          userSettings.set(userId, settings);
          
          await bot.answerCallbackQuery(query.id, {
            text: `🔔 Уведомления ${settings.notifications ? 'включены' : 'выключены'}`
          });
          break;
          
        case 'contacts':
          cache.set(`${chatId}_state`, 'awaiting_phone');
          
          await bot.sendMessage(
            chatId,
            '📱 Введите ваш номер телефона:',
            {
              reply_markup: {
                force_reply: true
              }
            }
          );
          break;
          
        case 'language':
          // For future implementation
          await bot.answerCallbackQuery(query.id, {
            text: '🌍 Эта функция будет доступна в следующем обновлении'
          });
          break;
      }
      
      // Update settings message
      await bot.editMessageText(
        '⚙️ *Настройки*\n\n' +
        `🔔 Уведомления: ${settings.notifications ? 'Включены' : 'Выключены'}\n` +
        `🌍 Язык: ${settings.language === 'ru' ? 'Русский' : 'English'}\n` +
        `📱 Телефон: ${settings.phone || 'Не указан'}\n` +
        `📧 Email: ${settings.email || 'Не указан'}`,
        {
          chat_id: chatId,
          message_id: query.message.message_id,
          parse_mode: 'Markdown',
          ...getSettingsKeyboard()
        }
      );
    }

    await bot.answerCallbackQuery(query.id);
  } catch (error) {
    logger.error('Error handling callback query', { error, action });
    await bot.sendMessage(chatId, 'Произошла ошибка при обработке запроса.');
  }
});

// Message handler for collecting additional information
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const state = cache.get(`${chatId}_state`);
  
  if (!state) return;
  
  try {
    if (state === 'awaiting_broadcast' && isAdmin(userId)) {
      const message = sanitizeInput(msg.text);
      const clients = new Set(Array.from(repairRequests.values()).map(req => req.userId));
      
      let successCount = 0;
      let failCount = 0;
      
      for (const clientId of clients) {
        try {
          await bot.sendMessage(clientId, `📢 *Важное сообщение:*\n\n${message}`, {
            parse_mode: 'Markdown'
          });
          successCount++;
        } catch (error) {
          failCount++;
          logger.error('Error sending broadcast message', { error, clientId });
        }
      }
      
      await bot.sendMessage(
        chatId,
        `📢 Рассылка завершена\n\n` +
        `✅ Успешно отправлено: ${successCount}\n` +
        `❌ Ошибок: ${failCount}`
      );
      
      cache.del(`${chatId}_state`);
    } else if (state.startsWith('awaiting_comment_')) {
      const requestId = parseInt(state.replace('awaiting_comment_', ''));
      const request = repairRequests.get(requestId);
      
      if (request && isAdmin(userId)) {
        request.comment = sanitizeInput(msg.text);
        
        await bot.sendMessage(
          chatId,
          '💬 Комментарий добавлен к заявке',
          {
            reply_markup: {
              inline_keyboard: [[{ text: '🔙 Назад к заявке', callback_data: `update_${requestId}` }]]
            }
          }
        );
        
        cache.del(`${chatId}_state`);
      }
    } else if (state.startsWith('awaiting_schedule_')) {
      const requestId = parseInt(state.replace('awaiting_schedule_', ''));
      const request = repairRequests.get(requestId);
      
      if (request && isAdmin(userId)) {
        const scheduledTime = moment(msg.text, 'DD.MM.YYYY HH:mm');
        
        if (scheduledTime.isValid()) {
          request.scheduledTime = scheduledTime.toDate();
          
          // Schedule notification for client
          const reminderTime = moment(scheduledTime).subtract(2, 'hours');
          if (reminderTime.isAfter(moment())) {
            scheduleReminder(request.userId, requestId, reminderTime.toDate());
          }
          
          // Notify client
          await bot.sendMessage(
            request.userId,
            `📅 Для вашей заявки #${requestId} назначено время:\n` +
            `${scheduledTime.format('DD.MM.YYYY HH:mm')}\n\n` +
            'Напоминание придет за 2 часа до назначенного времени.'
          );
          
          await bot.sendMessage(
            chatId,
            '📅 Время успешно назначено',
            {
              reply_markup: {
                inline_keyboard: [[{ text: '🔙 Назад к заявке', callback_data: `update_${requestId}` }]]
              }
            }
          );
        } else {
          await bot.sendMessage(
            chatId,
            '⚠️ Неверный формат даты и времени. Попробуйте еще раз (DD.MM.YYYY HH:mm):'
          );
          return;
        }
        
        cache.del(`${chatId}_state`);
      }
    } else if (state === 'awaiting_phone') {
      const phone = msg.text.trim();
      
      if (validatePhoneNumber(phone)) {
        const settings = userSettings.get(userId) || {};
        settings.phone = phone;
        userSettings.set(userId, settings);
        
        cache.set(`${chatId}_state`, 'awaiting_email');
        
        await bot.sendMessage(
          chatId,
          '📧 Теперь введите ваш email:'
        );
      } else {
        await bot.sendMessage(
          chatId,
          '⚠️ Неверный формат номера телефона. Попробуйте еще раз:'
        );
      }
    } else if (state === 'awaiting_email') {
      const email = msg.text.trim();
      
      if (validator.isEmail(email)) {
        const settings = userSettings.get(userId) || {};
        settings.email = email;
        userSettings.set(userId, settings);
        
        await bot.sendMessage(
          chatId,
          '✅ Контактные данные успешно обновлены!',
          {
            reply_markup: {
              inline_keyboard: [[{ text: '🔙 Назад к настройкам', callback_data: 'settings_profile' }]]
            }
          }
        );
        
        cache.del(`${chatId}_state`);
      } else {
        await bot.sendMessage(
          chatId,
          '⚠️ Неверный формат email. Попробуйте еще раз:'
        );
      }
    }
  } catch (error) {
    logger.error('Error handling message', { error, chatId, state });
    await bot.sendMessage(chatId, 'Произошла ошибка при обработке сообщения.');
    cache.del(`${chatId}_state`);
  }
});

// Helper function for status emoji
const getStatusEmoji = (status) => {
  const statusMap = {
    pending: '⏳ В ожидании',
    in_progress: '🔧 В работе',
    completed: '✅ Завершено',
    cancelled: '❌ Отменено'
  };
  return statusMap[status] || status;
};

// Error handler for unhandled rejections
process.on('unhandledRejection', (error) => {
  logger.error('Unhandled rejection', { error });
});

// Graceful shutdown
process.on('SIGTERM', () => {
  logger.info('Received SIGTERM signal');
  process.exit(0);
});

logger.info('PC Repair Bot started successfully');
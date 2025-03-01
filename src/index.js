require('dotenv').config();
const { Client, GatewayIntentBits, Partials, Collection, REST, Routes, AttachmentBuilder } = require('discord.js');
const interactionHandler = require('./interactionHandler');
const canvafy = require('canvafy');
const cron = require('node-cron');
const path = require('path');
const fs = require('fs');

const { incrementMessageLeaderWins, getUserData, saveUserData, listUserData, updateUserData } = require('./dynamoDB');
const { getConfig, saveConfig } = require('./configManager');

const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

const clientId = process.env.CLIENT_ID;
const token = process.env.TOKEN;
if (!clientId || !token) {
  console.error("Missing CLIENT_ID or TOKEN in .env file.");
  process.exit(1);
}

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages,
  ],
  partials: [Partials.Channel, Partials.Message, Partials.User],
});

client.commands = new Collection();
const commands = [];
const commandsPath = path.join(__dirname, 'commands');
const commandFiles = fs.readdirSync(commandsPath).filter(file => file.endsWith('.js'));

for (const file of commandFiles) {
  try {
    const filePath = path.join(commandsPath, file);
    const command = require(filePath);
    if (command.data) {
      client.commands.set(command.data.name, command);
      commands.push(command.data.toJSON());
      console.log(`Loaded command: ${command.data.name}`);
    } else {
      console.error(`Command ${file} is missing 'data' property and was not loaded.`);
    }
  } catch (error) {
    console.error(`Failed to load command ${file}: ${error.message}`);
  }
}

const streakCooldowns = new Map();
const userMessageData = {};

// -------------------------
// UTILITY FUNCTIONS
// -------------------------

// #TODO: figure out how to do this better, as of right now its not very accurate.
function getSimilarityScore(text1, text2) {
  const [shorter, longer] = text1.length < text2.length ? [text1, text2] : [text2, text1];
  const editDistance = levenshteinDistance(shorter, longer);
  return (longer.length - editDistance) / longer.length;
}
function levenshteinDistance(a, b) {
  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b[i - 1] === a[j - 1]) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[b.length][a.length];
}
function detectSpam(message, userId) {
  const currentTime = Date.now();
  const data = userMessageData[userId] || { lastMessage: null, lastTime: currentTime };
  const timeDifference = currentTime - data.lastTime;
  const similarityScore = data.lastMessage ? getSimilarityScore(data.lastMessage, message.content) : 0;
  if (timeDifference < 2500 && similarityScore > 0.85) {
    console.log(`[SPAM DETECTION] Rapidly sent similar messages.`);
    return true;
  }
  userMessageData[userId] = { lastMessage: message.content, lastTime: currentTime };
  return false;
}

// -------------------------
// GUILD CONFIG INITIALIZATION
// -------------------------

async function initializeGuildConfig(guildId) {
  let config = await getConfig(guildId);
  if (!config) {
    const initialConfig = {
      streakSystem: {
        enabled: false,
        streakThreshold: 4,
        isGymClassServer: false,
        enabledDate: new Date().toISOString(),
      },
      messageLeaderSystem: {
        enabled: false,
      },
      levelSystem: {
        enabled: false,
        xpPerMessage: 10,
        levelMultiplier: 1.5,
        levelUpMessages: true,
        rewards: {},
      },
      reportSettings: {
        weeklyReportChannel: "",
        monthlyReportChannel: "",
      },
      channels: {},
      roles: {},
    };
    await saveConfig(guildId, initialConfig);
    return { config: initialConfig, newlyCreated: true };
  }
  return { config, newlyCreated: false };
}

// -------------------------
// CLIENT EVENTS
// -------------------------

client.on('ready', async () => {
  console.log(`Logged in as ${client.user.tag}!`)

  const rest = new REST({ version: '10' }).setToken(token);
  try {
    await rest.put(Routes.applicationCommands(clientId), { body: commands });
    console.log('Successfully reloaded application (/) commands.');
  } catch (error) {
    console.error(`Error registering commands: ${error.message}`);
  }

  try {
    const guilds = client.guilds.cache.map(guild => guild.id);
    for (const guildId of guilds) {
      const { config, newlyCreated } = await initializeGuildConfig(guildId);
      if (newlyCreated) {
        const guild = client.guilds.cache.get(guildId);
        if (guild) {
          await sendConfigMessage(guild);
        }
      }
    }
  } catch (error) {
    console.error(`Error during guild initialization: ${error.message}`);
  }

  scheduleDailyReset();
  setTimeout(scheduleMessageLeaderAnnounce, 2000);
  scheduleWeeklyReport();
  scheduleMonthlyReport();
});

client.on('interactionCreate', async interaction => {
  try {
    await interactionHandler(client, interaction);
  } catch (error) {
    console.error(`Error handling interaction: ${error}`);
  }
});

client.on('guildCreate', async guild => {
  try {
    const { newlyCreated } = await initializeGuildConfig(guild.id);
    if (newlyCreated) {
      await sendConfigMessage(guild);
    }
  } catch (error) {
    console.error(`Error during guild creation: ${error.message}`);
  }
});

// -------------------------
// TIMEOUT HELPER
// -------------------------

function setLongTimeout(callback, duration) {
  const maxDuration = 2147483647;
  if (duration > maxDuration) {
    setTimeout(() => {
      setLongTimeout(callback, duration - maxDuration);
    }, maxDuration);
  } else {
    setTimeout(callback, duration);
  }
}

// -------------------------
// SCHEDULED TASKS
// -------------------------

function scheduleDailyReset() {
  cron.schedule('0 0 * * *', () => {
    console.log('Running daily streak reset at 12 AM');
    resetDailyStreaks();
  });
}

function scheduleMessageLeaderAnnounce() {
  cron.schedule('0 18 * * 0', () => {
    console.log('Running weekly message leader announcement');
    announceMessageLeaders();
  });
}

function scheduleWeeklyReport() {
  cron.schedule('0 18 * * 0', async () => {
    const guilds = client.guilds.cache.map(guild => guild.id);
    for (const guildId of guilds) {
      await generateWeeklyReport(guildId);
    }
    console.log('Weekly report generated at 12 AM on Sunday');
  });
}

function scheduleMonthlyReport() {
  const monthlyInterval = 30 * 24 * 60 * 60 * 1000;
  const guilds = client.guilds.cache.map(guild => guild.id);
  for (const guildId of guilds) {
    setLongTimeout(async () => {
      await generateMonthlyReport(guildId);
      scheduleMonthlyReport();
    }, monthlyInterval);
  }
}

// -------------------------
// ANNOUNCEMENT AND REPORT FUNCTIONS
// -------------------------

async function announceMessageLeaders() {
  try {
    const guilds = client.guilds.cache.map(guild => guild.id);
    for (const guildId of guilds) {
      const config = await getConfig(guildId);
      if (!config || !config.messageLeaderSystem || !config.messageLeaderSystem.enabled) continue;
      const users = await listUserData();
      const currentGuild = client.guilds.cache.get(guildId);
      if (!currentGuild) continue;
      const currentMembers = await currentGuild.members.fetch();
      const messageLeaders = users
        .filter(({ userId, userData }) => currentMembers.has(userId) && userData.messages > 0)
        .sort((a, b) => b.userData.messages - a.userData.messages)
        .slice(0, 10);
      if (messageLeaders.length === 0) continue;
      const top10Users = messageLeaders.map(({ userId, userData }, index) => {
        const member = currentGuild.members.cache.get(userId);
        return {
          top: index + 1,
          avatar: member ? member.user.displayAvatarURL({ format: 'png' }) : '',
          tag: member ? member.user.username : 'N/A',
          score: userData.messages
        };
      });
      const leaderboardImage = await new canvafy.Top()
        .setOpacity(0.6)
        .setScoreMessage("Messages:")
        .setBackground(
          'image',
          'https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg'
        )
        .setColors({
          box: '#212121',
          username: '#ffffff',
          score: '#ffffff',
          firstRank: '#f7c716',
          secondRank: '#9e9e9e',
          thirdRank: '#94610f',
        })
        .setUsersData(top10Users)
        .build();
      const attachment = new AttachmentBuilder(leaderboardImage, { name: `leaderboard-${guildId}.png` });
      const messageLeaderChannelId = config.messageLeaderSystem.channelMessageLeader;
      const messageLeaderChannel = currentGuild.channels.cache.get(messageLeaderChannelId);
      if (!messageLeaderChannel || !messageLeaderChannel.isTextBased()) {
        console.error(`Message leader channel is not valid or not text-based for guild ${guildId}.`);
        continue;
      }
      let messageContent = '';
      if (config.streakSystem && config.streakSystem.isGymClassServer) {
        messageContent = `Whats up Gym Class!! Here are the Message Leader winners for last week!! 💪🔥\n\n`;
        messageContent += `🏆 1st: <@${messageLeaders[0]?.userId || ''}> (${top10Users[0]?.tag || 'N/A'})\n`;
        messageContent += `🥈 2nd: <@${messageLeaders[1]?.userId || ''}> (${top10Users[1]?.tag || 'N/A'})\n`;
        messageContent += `🥉 3rd: <@${messageLeaders[2]?.userId || ''}> (${top10Users[2]?.tag || 'N/A'})\n`;
        messageContent += `💪 4th: <@${messageLeaders[3]?.userId || ''}> (${top10Users[3]?.tag || 'N/A'})\n`;
        messageContent += `🔥 5th: <@${messageLeaders[4]?.userId || ''}> (${top10Users[4]?.tag || 'N/A'})\n`;
        messageContent += `📜 (6th-10th): ${messageLeaders.slice(5).map(({ userId }, index) => `<@${userId}> (${top10Users[index + 5]?.tag || 'N/A'})`).join(', ')}\n\n`;
        messageContent += `**To those who have the message leader role this week, to claim your hat, please use the /claim_role command in the appropriate channel!**`;
      } else {
        messageContent = `🎉 **Message Leaders for last week in ${currentGuild.name}!** 🔥\n\n`;
        messageContent += `🏆 1st: <@${messageLeaders[0]?.userId || ''}> (${top10Users[0]?.tag || 'N/A'})\n`;
        messageContent += `🥈 2nd: <@${messageLeaders[1]?.userId || ''}> (${top10Users[1]?.tag || 'N/A'})\n`;
        messageContent += `🥉 3rd: <@${messageLeaders[2]?.userId || ''}> (${top10Users[2]?.tag || 'N/A'})\n`;
        messageContent += `🎖️ 4th: <@${messageLeaders[3]?.userId || ''}> (${top10Users[3]?.tag || 'N/A'})\n`;
        messageContent += `🎖️ 5th: <@${messageLeaders[4]?.userId || ''}> (${top10Users[4]?.tag || 'N/A'})\n`;
        messageContent += `📜 6th-10th: ${messageLeaders.slice(5).map(({ userId }, index) => `<@${userId}> (${top10Users[index + 5]?.tag || 'N/A'})`).join(', ')}\n\n`;
        messageContent += `Congratulations to everyone who participated!`;
      }
      try {
        await messageLeaderChannel.send({ content: messageContent, files: [attachment] });
      } catch (error) {
        console.error(`Failed to send message leaders to guild ${guildId}: ${error.message}`);
      }

      const leaderRoleId = config.messageLeaderSystem.roleMessageLeader;
      if (leaderRoleId) {
        const leaderRole = currentGuild.roles.cache.get(leaderRoleId);
        if (leaderRole) {
          for (const member of leaderRole.members.values()) {
            try {
              await member.roles.remove(leaderRole);
            } catch (error) {
              console.error(`Failed to remove leader role from ${member.user.tag} in guild ${guildId}: ${error.message}`);
            }
          }
          for (let i = 0; i < 5; i++) {
            const userId = messageLeaders[i]?.userId;
            if (userId) {
              try {
                await assignRole(guildId, userId, leaderRoleId);
              } catch (error) {
                console.error(`Failed to assign leader role to user ${userId} in guild ${guildId}: ${error.message}`);
              }
            }
          }
        } else {
          console.error(`Leader role ID ${leaderRoleId} not found in guild ${guildId}`);
        }
      } else {
        console.warn(`No leader role configured for guild ${guildId}`);
      }
      for (let i = 0; i < 5; i++) {
        const userId = messageLeaders[i]?.userId;
        if (userId) {
          try {
            await incrementMessageLeaderWins(userId);
          } catch (err) {
            console.error(`Error incrementing wins for user ${userId}:`, err);
          }
        }
      }
      for (const { userId, userData } of users) {
        userData.messages = 0;
        await saveUserData(userId, userData);
      }
    }
  } catch (error) {
    console.error(`Error during message leader announcement: ${error.message}`);
  }
}

async function assignRole(guildId, userId, roleId) {
  try {
    const guild = client.guilds.cache.get(guildId);
    const member = await guild.members.fetch(userId);
    if (member && roleId) {
      const role = guild.roles.cache.get(roleId);
      if (role) {
        await member.roles.add(role);
        return role;
      } else {
        console.warn(`Role with ID ${roleId} not found in guild ${guildId}`);
      }
    } else {
      console.warn(`Member with ID ${userId} not found in guild ${guildId}`);
    }
  } catch (error) {
    console.error(`Failed to assign role ${roleId} to user ${userId} in guild ${guildId}: ${error.message}`);
  }
  return null;
}

// -------------------------
// MESSAGE HANDLING
// -------------------------

client.on('messageCreate', async (message) => {
  if (!message.guild || message.author.bot) return;
  const userId = message.author.id;

  await handleUserMessage(message.guild.id, userId, message.channel, message);
});

async function handleUserMessage(guildId, userId, channel, message) {
  try {
    let userData = await getUserData(userId);
    if (!userData) {
      const config = await getConfig(guildId);
      userData = {
        streak: 0,
        highestStreak: 0,
        messages: 0,
        threshold: config && config.streakSystem ? parseInt(config.streakSystem.streakThreshold) || 10 : 10,
        receivedDaily: false,
        messageLeaderWins: 0,
        highestMessageCount: 0,
        mostConsecutiveLeader: 0,
        totalMessages: 0,
        daysTracked: 0,
        averageMessagesPerDay: 0,
        activeDaysCount: 0,
        longestInactivePeriod: 0,
        lastStreakLoss: null,
        messageHeatmap: [],
        milestones: [],
        rolesAchieved: [],
        experience: { totalXp: 0, level: 0 },
        boosters: 1,
        lastMessage: { time: 0, content: '', date: null },
        channelsParticipated: [],
        mentionsRepliesCount: { mentions: 0, replies: 0 }
      };
    }
    const now = Date.now();
    const today = new Date().toISOString().split('T')[0];
    const streakCooldown = 3000;
    const lastStreakUpTime = streakCooldowns.get(userId) || 0;
    userData.lastMessage = { time: now, content: message.content, date: today };
    if (!Array.isArray(userData.channelsParticipated)) {
      userData.channelsParticipated = [];
    }
    if (!userData.channelsParticipated.includes(channel.id)) {
      userData.channelsParticipated.push(channel.id);
    }
    if (message.mentions?.users?.has(userId)) {
      userData.mentionsRepliesCount.mentions += 1;
    }
    if (message.type === 'REPLY') {
      userData.mentionsRepliesCount.replies += 1;
    }
    if (now - lastStreakUpTime < streakCooldown) return;
    if (detectSpam(message, userId)) return;

    const config = await getConfig(guildId);
    if (config && config.levelSystem && config.levelSystem.enabled) {
      const xpGain = Math.max(0, config.levelSystem.xpPerMessage * (userData.boosters || 1));
      userData.experience.totalXp += xpGain;
      const xpRequired = Math.floor(100 * Math.pow(config.levelSystem.levelMultiplier, userData.experience.level));
      if (userData.experience.totalXp >= xpRequired) {
        userData.experience.level++;
        userData.experience.totalXp -= xpRequired;
        const rewardRoleKey = `roleLevel${userData.experience.level}`;
        const rewardRole = config.levelSystem[rewardRoleKey];
        if (!rewardRole) {
          console.error(`No reward defined for level ${userData.experience.level} in guild ${guildId}`);
        } else {
          await assignRole(guildId, userId, rewardRole);
        }
        const levelUpChannelId = config.levelSystem.channelLevelUp || channel.id;
        const levelUpChannel = channel.guild.channels.cache.get(levelUpChannelId) || channel;
        if (levelUpChannel && levelUpChannel.isTextBased()) {
          await levelUpChannel.send(`🎉 <@${userId}> has leveled up to level ${userData.experience.level}!`);
        }
      }
    }

    if (config && config.streakSystem && config.streakSystem.enabled) {
      if (userData.threshold > 0) {
        userData.threshold -= 1;
      }
      if (userData.threshold === 0 && !userData.receivedDaily) {
        userData.streak += 1;
        userData.receivedDaily = true;
        if (userData.streak > userData.highestStreak) {
          userData.highestStreak = userData.streak;
        }
        const streakChannelId = config.streakSystem.channelStreakOutput || channel.id;
        const streakChannel = channel.guild.channels.cache.get(streakChannelId);
        let milestoneRole = null;
        let milestone = 0;
        for (const key in config.streakSystem) {
          if (key.startsWith('role') && key.endsWith('day')) {
            const streakDays = parseInt(key.replace('role', '').replace('day', ''));
            if (userData.streak === streakDays) {
              milestone = streakDays;
              milestoneRole = await assignRole(guildId, userId, config.streakSystem[key]);
              break;
            }
          }
        }
        if (milestone > 0) {
          userData.milestones.push({ milestone, date: new Date().toISOString() });
          if (milestoneRole) {
            userData.rolesAchieved.push(milestoneRole.name);
          }
        }
        const streakUpImage = await new canvafy.LevelUp()
          .setAvatar(channel.guild.members.cache.get(userId).user.displayAvatarURL())
          .setBackground(
            "image",
            "https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg"
          )
          .setUsername(channel.guild.members.cache.get(userId).user.username)
          .setBorder("#FF0000")
          .setAvatarBorder("#FFFFFF")
          .setOverlayOpacity(0.7)
          .setLevels(userData.streak - 1, userData.streak)
          .build();
        let streakMessage = `🎉 <@${userId}> has upped their streak to ${userData.streak}!!`;
        if (milestoneRole) {
          streakMessage += ` They now have the ${milestone} Day Streak Role!`;
        }
        if (streakChannel && streakChannel.isTextBased()) {
          await streakChannel.send({
            content: streakMessage,
            files: [{ attachment: streakUpImage, name: `streak-${userId}.png` }],
          });
        }
        streakCooldowns.set(userId, now);
      }
    }

    userData.messages += 1;
    userData.totalMessages += 1;
    if (!Array.isArray(userData.messageHeatmap)) {
      userData.messageHeatmap = [];
    }
    const lastHeatmapEntry = userData.messageHeatmap.find(entry => entry.date === today);
    if (lastHeatmapEntry) {
      lastHeatmapEntry.messages += 1;
    } else {
      userData.messageHeatmap.push({ date: today, messages: 1 });
    }
    await updateUserData(userId, {
      streak: userData.streak,
      receivedDaily: true,
      highestStreak: userData.highestStreak,
      messages: userData.messages,
      totalMessages: userData.totalMessages,
    });
  } catch (error) {
    console.error(`Error handling user message for user ${userId} in guild ${guildId}: ${error.message}`);
  }
}

async function resetDailyStreaks() {
  try {
    const guilds = client.guilds.cache.map(guild => guild.id);
    for (const guildId of guilds) {
      const config = await getConfig(guildId);
      if (!config) continue;
      const messageThreshold = config.streakSystem.streakThreshold || 10;
      const today = new Date().toISOString().split('T')[0];
      const users = await listUserData();
      for (const { userId, userData } of users) {
        if (!userData.messageHeatmap.some(entry => entry.date === today)) {
          userData.messageHeatmap.push({ date: today, messages: 0 });
        }
        if (userData.receivedDaily) {
          userData.receivedDaily = false;
        }
        if (userData.threshold !== messageThreshold) {
          userData.threshold = messageThreshold;
        }
        if (userData.streak > 0 && userData.threshold > 0 && !userData.receivedDaily) {
          const oldStreak = userData.streak;
          userData.streak = 0;
          await removeStreakRoles(guildId, userId, config, oldStreak);
          userData.lastStreakLoss = new Date().toISOString();
        }
        userData.daysTracked = (userData.daysTracked || 0) + 1;
        userData.totalMessages = (userData.totalMessages || 0) + (userData.dailyMessageCount || 0);
        userData.averageMessagesPerDay = userData.totalMessages / userData.daysTracked;
        userData.dailyMessageCount = 0;
        if (userData.dailyMessageCount === 0) {
          const lastActiveDate = userData.messageHeatmap.length > 0
              ? new Date(userData.messageHeatmap[userData.messageHeatmap.length - 1].date)
              : new Date();
          const inactiveDays = Math.floor((new Date() - lastActiveDate) / (1000 * 60 * 60 * 24));
          userData.longestInactivePeriod = Math.max(userData.longestInactivePeriod || 0, inactiveDays);
        }
        if (new Date().getDay() === 0) {
          userData.messagesInCurrentWeek = 0;
        }
        await updateUserData(userId, {
          receivedDaily: false,
          threshold: messageThreshold,
          messagesInCurrentWeek: new Date().getDay() === 0 ? 0 : userData.messagesInCurrentWeek,
        });
      }
    }
  } catch (error) {
    console.error(`Error during daily streak reset: ${error.message}`);
  }
}

async function removeStreakRoles(guildId, userId, config, oldStreak) {
  try {
    const guild = client.guilds.cache.get(guildId);
    let member;
    try {
      member = await guild.members.fetch(userId);
    } catch (error) {
      console.error(`Failed to fetch member with ID ${userId} in guild ${guildId}: ${error.message}`);
      return;
    }
    if (!member) {
      console.error(`Could not find member with ID ${userId} in guild ${guildId}.`);
      return;
    }
    const rolesToRemove = [];
    for (const key in config.streakSystem) {
      if (key.startsWith('role') && key.endsWith('day')) {
        const streakDays = parseInt(key.replace('role', '').replace('day', ''));
        if (oldStreak >= streakDays) {
          const role = guild.roles.cache.get(config.streakSystem[key]);
          if (role && member.roles.cache.has(role.id)) {
            rolesToRemove.push(role.id);
          }
        }
      }
    }
    const removalMessage = `You failed to send your required messages yesterday and therefore lost your ${oldStreak}-day message streak in the ${guild.name} server!`;
    try {
      await member.send(removalMessage);
    } catch (error) {
      console.error(`Failed to send DM to user ${userId} in guild ${guildId}: ${error.message}`);
      if (error.code === 50007) {
        console.warn(`User ${userId} has DMs disabled or has blocked the bot.`);
      } else {
        console.error(`Unexpected error when sending DM to user ${userId}: ${error.message}`);
      }
      const streakChannelId = config.streakSystem.channelStreakOutput;
      const streakChannel = guild.channels.cache.get(streakChannelId);
      if (streakChannel && streakChannel.isTextBased()) {
        await streakChannel.send(`I couldn't DM <@${userId}> about their streak loss. They might have DMs disabled.`);
      }
    }
    if (rolesToRemove.length > 0) {
      await member.roles.remove(rolesToRemove);
    }
  } catch (error) {
    console.error(`Error removing streak roles in guild ${guildId} for user ${userId}: ${error.message}`);
  }
}

// -------------------------
// CONFIGURATION MESSAGE
// -------------------------

async function sendConfigMessage(guild) {
  try {
    let targetChannel = null;
    if (guild.publicUpdatesChannelId) {
      targetChannel = guild.channels.cache.get(guild.publicUpdatesChannelId);
    }
    if (!targetChannel && guild.systemChannelId) {
      targetChannel = guild.channels.cache.get(guild.systemChannelId);
    }
    if (!targetChannel) {
      targetChannel = guild.channels.cache.find(channel => channel.isTextBased());
    }
    if (targetChannel) {
      const auditLogs = await guild.fetchAuditLogs({ type: 28, limit: 1 });
      const botAddLog = auditLogs.entries.first();
      const userWhoAddedBot = botAddLog ? botAddLog.executor : null;
      let messageContent = "Hello! To set up the Streak Bot, please use `/setup-bot` to configure the systems.";
      if (userWhoAddedBot) {
        messageContent = `Hello ${userWhoAddedBot}, to set up the Streak Bot, please use \`/setup-bot\` to configure the systems.`;
      }
      await targetChannel.send(messageContent);
    }
  } catch (error) {
    console.error(`Failed to send configuration message in guild ${guild.id}: ${error.message}`);
  }
}

// -------------------------
// REPORT GENERATION
// -------------------------

async function generateWeeklyReport(guildId) {
  try {
    const config = await getConfig(guildId);
    if (!config) return;
    const users = await listUserData();
    const totalMessages = users.reduce((acc, { userData }) => acc + (userData.messages || 0), 0);
    const totalUsers = users.length;
    const averageMessagesPerUser = (totalMessages / totalUsers).toFixed(2);
    const guild = client.guilds.cache.get(guildId);
    const reportChannelId = config.reportSettings.weeklyReportChannel;
    const reportChannel = guild.channels.cache.get(reportChannelId);
    if (!reportChannel || !reportChannel.isTextBased()) {
      console.error(`Invalid weekly report channel for guild ${guildId}.`);
      return;
    }
    const reportMessage = `**Weekly Report for ${guild.name}**\n\n` +
      `- Total Messages: ${totalMessages}\n` +
      `- Total Active Users: ${totalUsers}\n` +
      `- Average Messages per User: ${averageMessagesPerUser}`;
    await reportChannel.send(reportMessage);
  } catch (error) {
    console.error(`Error generating weekly report for guild ${guildId}: ${error.message}`);
  }
}

async function generateMonthlyReport(guildId) {
  try {
    const config = await getConfig(guildId);
    if (!config) return;
    const users = await listUserData();
    const totalMessages = users.reduce((acc, { userData }) => acc + (userData.messages || 0), 0);
    const totalUsers = users.length;
    const averageMessagesPerUser = (totalMessages / totalUsers).toFixed(2);
    const guild = client.guilds.cache.get(guildId);
    const reportChannelId = config.reportSettings.monthlyReportChannel;
    const reportChannel = guild.channels.cache.get(reportChannelId);
    if (!reportChannel || !reportChannel.isTextBased()) {
      console.error(`Invalid monthly report channel for guild ${guildId}.`);
      return;
    }
    const reportMessage = `**Monthly Report for ${guild.name}**\n\n` +
      `- Total Messages: ${totalMessages}\n` +
      `- Total Active Users: ${totalUsers}\n` +
      `- Average Messages per User: ${averageMessagesPerUser}`;
    await reportChannel.send(reportMessage);
  } catch (error) {
    console.error(`Error generating monthly report for guild ${guildId}: ${error.message}`);
  }
}

client.on('guildMemberRemove', async () => {
  // when becomes necessary add logic here to keep things clean,
});

client.login(token).catch(console.error);
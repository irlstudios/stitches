require('dotenv').config();
const { Client, GatewayIntentBits, Partials, Collection, REST, Routes, AttachmentBuilder } = require('discord.js');
const interactionHandler = require('./interactionHandler');
const canvafy = require('canvafy');
const cron = require('node-cron');
const path = require('path');
const fs = require('fs');
const { getUserData, saveUserData, listUserData, updateUserData, incrementMessageLeaderWins } = require('./dynamoDB');
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
  console.log(`Logged in as ${client.user.tag}!`);

  const rest = new (require('@discordjs/rest').REST)({ version: '10' }).setToken(token);
  try {
    await rest.put(require('discord-api-types/v10').Routes.applicationCommands(clientId), { body: commands });
    console.log('Successfully reloaded application (/) commands.');
  } catch (error) {
    console.error(`Error registering commands: ${error.message}`);
  }

  try {
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      const { newlyCreated } = await initializeGuildConfig(gId);
      if (newlyCreated) {
        const gObj = client.guilds.cache.get(gId);
        if (gObj) {
          await sendConfigMessage(gObj);
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

client.on('messageCreate', async message => {
  if (!message.guild || message.author.bot) return;
  if (detectSpam(message, message.author.id)) return;
  await handleUserMessage(message.guild.id, message.author.id, message.channel, message);
});

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
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      await generateWeeklyReport(gId);
    }
    console.log('Weekly report done');
  });
}

function scheduleMonthlyReport() {
  const monthlyInterval = 30 * 24 * 60 * 60 * 1000;
  const guilds = client.guilds.cache.map(g => g.id);
  for (const gId of guilds) {
    setLongTimeout(async () => {
      await generateMonthlyReport(gId);
      scheduleMonthlyReport();
    }, monthlyInterval);
  }
}

// -------------------------
// ANNOUNCEMENT AND REPORT FUNCTIONS
// -------------------------

async function announceMessageLeaders() {
  try {
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      const config = await getConfig(gId);
      if (!config || !config.messageLeaderSystem?.enabled) continue;
      const allItems = await listUserData(gId);
      const currentGuild = client.guilds.cache.get(gId);
      if (!currentGuild) continue;
      const members = await currentGuild.members.fetch();

      const withMsgs = allItems.filter(i => i.userData && i.userData.messages && members.has(i.userId));
      if (!withMsgs.length) continue;
      withMsgs.sort((a, b) => b.userData.messages - a.userData.messages);
      const top10 = withMsgs.slice(0, 10);

      if (!top10.length) continue;

      const top10Users = top10.map((item, idx) => {
        const mem = currentGuild.members.cache.get(item.userId);
        return {
          top: idx + 1,
          avatar: mem ? mem.user.displayAvatarURL({ format: 'png' }) : '',
          tag: mem ? mem.user.username : 'N/A',
          score: item.userData.messages
        };
      });

      const image = await new canvafy.Top()
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

      const attach = new AttachmentBuilder(image, { name: `leaderboard-${gId}.png` });
      const chanId = config.messageLeaderSystem.channelMessageLeader;
      const chan = currentGuild.channels.cache.get(chanId);
      if (!chan || !chan.isTextBased()) continue;

      let msgContent = '';
      if (config.streakSystem?.isGymClassServer) {
        msgContent = `Whats up Gym Class!! Here are the Message Leader winners for last week!! 💪🔥\n\n`;
        msgContent += `🏆 1st: <@${top10[0]?.userId}> (${top10Users[0].tag})\n`;
        if (top10[1]) msgContent += `🥈 2nd: <@${top10[1].userId}> (${top10Users[1].tag})\n`;
        if (top10[2]) msgContent += `🥉 3rd: <@${top10[2].userId}> (${top10Users[2].tag})\n`;
        // etc, truncated for brevity
      } else {
        msgContent = `🎉 **Message Leaders for last week in ${currentGuild.name}!** 🔥\n\n`;
        msgContent += `🏆 1st: <@${top10[0]?.userId}> (${top10Users[0].tag})\n`;
        if (top10[1]) msgContent += `🥈 2nd: <@${top10[1].userId}> (${top10Users[1].tag})\n`;
        if (top10[2]) msgContent += `🥉 3rd: <@${top10[2].userId}> (${top10Users[2].tag})\n`;
      }

      try {
        await chan.send({ content: msgContent, files: [attach] });
      } catch (err) {
        console.error(`Failed to send leader to guild ${gId}: ${err}`);
      }

      const leaderRoleId = config.messageLeaderSystem.roleMessageLeader;
      if (leaderRoleId) {
        const roleObj = currentGuild.roles.cache.get(leaderRoleId);
        if (roleObj) {
          for (const mem of roleObj.members.values()) {
            try {
              await mem.roles.remove(roleObj);
            } catch (err) {
              console.error(`Error removing role from ${mem.user.username}: ${err}`);
            }
          }
          for (let i = 0; i < Math.min(top10.length, 5); i++) {
            try {
              await assignRole(gId, top10[i].userId, leaderRoleId);
            } catch (err) {
              console.error(`Error awarding role: ${err}`);
            }
          }
        }
      }

      for (let i = 0; i < Math.min(top10.length, 5); i++) {
        try {
          await incrementMessageLeaderWins(top10[i].userId);
        } catch (err) {
          console.error(`Increment wins error: ${err}`);
        }
      }

      for (const it of allItems) {
        if (it.userData && it.userData.messages) {
          it.userData.messages = 0;
          await saveUserData(it.userId, it.userData);
        }
      }
    }
  } catch (error) {
    console.error(`announceMessageLeaders error: ${error}`);
  }
}

async function assignRole(guildId, userId, roleId) {
  try {
    const g = client.guilds.cache.get(guildId);
    const mem = await g.members.fetch(userId);
    if (!mem) return;
    const r = g.roles.cache.get(roleId);
    if (r) {
      await mem.roles.add(r);
    }
  } catch (err) {
    console.error(`assignRole error: ${err}`);
  }
}

// -------------------------
// MESSAGE HANDLING
//

async function handleUserMessage(guildId, userId, channel, message) {
  try {
    let userRec = await getUserData(guildId, userId);
    if (!userRec) {
      const c = await getConfig(guildId);
      userRec = {
        streak: 0,
        highestStreak: 0,
        messages: 0,
        threshold: c && c.streakSystem ? parseInt(c.streakSystem.streakThreshold) || 10 : 10,
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
    const sCool = streakCooldowns.get(userId) || 0;
    userRec.lastMessage = { time: now, content: message.content, date: today };

    if (!Array.isArray(userRec.channelsParticipated)) {
      userRec.channelsParticipated = [];
    }
    if (!userRec.channelsParticipated.includes(channel.id)) {
      userRec.channelsParticipated.push(channel.id);
    }

    if (message.mentions?.users?.has(userId)) {
      userRec.mentionsRepliesCount.mentions += 1;
    }
    if (message.type === 'REPLY') {
      userRec.mentionsRepliesCount.replies += 1;
    }

    if (now - sCool < 3000) return;

    streakCooldowns.set(userId, now);

    const c = await getConfig(guildId);

    if (c?.levelSystem?.enabled) {
      const xpGain = Math.max(0, c.levelSystem.xpPerMessage * (userRec.boosters || 1));
      userRec.experience.totalXp += xpGain;
      const xpReq = Math.floor(100 * Math.pow(c.levelSystem.levelMultiplier, userRec.experience.level));
      if (userRec.experience.totalXp >= xpReq) {
        userRec.experience.level++;
        userRec.experience.totalXp -= xpReq;
        const roleK = `roleLevel${userRec.experience.level}`;
        const rVal = c.levelSystem[roleK];
        if (rVal) {
          await assignRole(guildId, userId, rVal);
        }
        const outChanId = c.levelSystem.channelLevelUp || channel.id;
        const outChan = channel.guild.channels.cache.get(outChanId) || channel;
        if (outChan?.isTextBased()) {
          await outChan.send(`🎉 <@${userId}> has leveled up to level ${userRec.experience.level}!`);
        }
      }
    }

    if (c?.streakSystem?.enabled) {
      if (userRec.threshold > 0) {
        userRec.threshold -= 1;
      }
      if (userRec.threshold === 0 && !userRec.receivedDaily) {
        userRec.streak++;
        userRec.receivedDaily = true;
        if (userRec.streak > userRec.highestStreak) {
          userRec.highestStreak = userRec.streak;
        }
        const stChanId = c.streakSystem.channelStreakOutput || channel.id;
        const stChan = channel.guild.channels.cache.get(stChanId);

        let milestone = 0;
        let milestoneRole = null;
        for (const key in c.streakSystem) {
          if (key.startsWith('role') && key.endsWith('day')) {
            const days = parseInt(key.replace('role', '').replace('day', ''));
            if (userRec.streak === days) {
              milestone = days;
              milestoneRole = c.streakSystem[key];
              await assignRole(guildId, userId, milestoneRole);
              break;
            }
          }
        }

        if (milestone) {
          if (!Array.isArray(userRec.milestones)) userRec.milestones = [];
          userRec.milestones.push({ milestone, date: new Date().toISOString() });
        }

        const lvlImage = await new canvafy.LevelUp()
            .setAvatar(channel.guild.members.cache.get(userId).user.displayAvatarURL())
            .setBackground(
                "image",
                "https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg"
            )
            .setUsername(channel.guild.members.cache.get(userId).user.username)
            .setBorder("#FF0000")
            .setAvatarBorder("#FFFFFF")
            .setOverlayOpacity(0.7)
            .setLevels(userRec.streak - 1, userRec.streak)
            .build();

        let sMsg = `🎉 <@${userId}> has upped their streak to ${userRec.streak}!!`;
        if (milestoneRole) {
          sMsg += ` They now have the ${milestone} Day Streak Role!`;
        }
        if (stChan?.isTextBased()) {
          await stChan.send({
            content: sMsg,
            files: [{ attachment: lvlImage, name: `streak-${userId}.png` }],
          });
        }
      }
    }

    userRec.messages++;
    userRec.totalMessages++;

    if (!Array.isArray(userRec.messageHeatmap)) {
      userRec.messageHeatmap = [];
    }
    const dayObj = userRec.messageHeatmap.find(x => x.date === today);
    if (dayObj) {
      dayObj.messages++;
    } else {
      userRec.messageHeatmap.push({ date: today, messages: 1 });
    }

    await updateUserData(userId, {
      streak: userRec.streak,
      receivedDaily: true,
      highestStreak: userRec.highestStreak,
      messages: userRec.messages,
      totalMessages: userRec.totalMessages,
    });
  } catch (error) {
    console.error(`Error handling user message for user ${userId} in guild ${guildId}: ${error.message}`);
  }
}

async function resetDailyStreaks() {
  try {
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      const c = await getConfig(gId);
      if (!c) continue;
      const thresh = c.streakSystem?.streakThreshold || 10;
      const today = new Date().toISOString().split('T')[0];
      const all = await listUserData(gId);

      for (const { userId, userData } of all) {
        if (!Array.isArray(userData.messageHeatmap)) {
          userData.messageHeatmap = [];
        }
        if (!userData.messageHeatmap.some(x => x.date === today)) {
          userData.messageHeatmap.push({ date: today, messages: 0 });
        }

        if (userData.receivedDaily) {
          userData.receivedDaily = false;
        }
        if (userData.threshold !== thresh) {
          userData.threshold = thresh;
        }

        if (userData.streak > 0 && userData.threshold > 0 && !userData.receivedDaily) {
          const old = userData.streak;
          userData.streak = 0;
          await removeStreakRoles(gId, userId, c, old);
          userData.lastStreakLoss = new Date().toISOString();
        }

        userData.daysTracked = (userData.daysTracked || 0) + 1;
        userData.totalMessages = (userData.totalMessages || 0) + (userData.dailyMessageCount || 0);
        userData.averageMessagesPerDay = userData.totalMessages / userData.daysTracked;
        userData.dailyMessageCount = 0;

        if (userData.dailyMessageCount === 0) {
          const lastActiveDate = userData.messageHeatmap.length
              ? new Date(userData.messageHeatmap[userData.messageHeatmap.length - 1].date)
              : new Date();
          const inactiveDays = Math.floor((Date.now() - lastActiveDate.getTime()) / (1000 * 60 * 60 * 24));
          userData.longestInactivePeriod = Math.max(userData.longestInactivePeriod || 0, inactiveDays);
        }

        if (new Date().getDay() === 0) {
          userData.messagesInCurrentWeek = 0;
        }

        await updateUserData(userId, {
          receivedDaily: false,
          threshold: thresh,
          messagesInCurrentWeek: new Date().getDay() === 0 ? 0 : userData.messagesInCurrentWeek
        });
      }
    }
  } catch (error) {
    console.error(`Error during daily streak reset: ${error.message}`);
  }
}

async function removeStreakRoles(guildId, userId, cfg, oldStreak) {
  try {
    const g = client.guilds.cache.get(guildId);
    if (!g) return;
    const mem = await g.members.fetch(userId);
    if (!mem) return;
    const rolesToRem = [];
    for (const k in cfg.streakSystem) {
      if (k.startsWith('role') && k.endsWith('day')) {
        const d = parseInt(k.replace('role', '').replace('day', ''));
        if (oldStreak >= d) {
          const r = g.roles.cache.get(cfg.streakSystem[k]);
          if (r && mem.roles.cache.has(r.id)) {
            rolesToRem.push(r.id);
          }
        }
      }
    }
    try {
      await mem.send(`You failed to send your required messages and lost your ${oldStreak}-day message streak in ${g.name}!`);
    } catch (err) {
      console.warn(`Failed DM to ${userId} for streak loss: ${err}`);
    }
    if (rolesToRem.length) {
      await mem.roles.remove(rolesToRem);
    }
  } catch (err) {
    console.error(`removeStreakRoles error: ${err}`);
  }
}

// -------------------------
// CONFIGURATION MESSAGE
// -------------------------

async function sendConfigMessage(guild) {
  try {
    let c = guild.publicUpdatesChannel
        || guild.systemChannel
        || guild.channels.cache.find(x => x.isTextBased());
    if (c) {
      const logs = await guild.fetchAuditLogs({ type: 28, limit: 1 });
      const entry = logs.entries.first();
      const adder = entry ? entry.executor : null;
      let msg = "Hello! To set up the Streak Bot, please use `/setup-bot`.";
      if (adder) {
        msg = `Hello ${adder}, to set up the Streak Bot, please use \`/setup-bot\`.`;
      }
      await c.send(msg);
    }
  } catch (err) {
    console.error(`sendConfigMessage error in guild ${guild.id}: ${err}`);
  }
}

async function generateWeeklyReport(guildId) {
  try {
    const c = await getConfig(guildId);
    if (!c) return;
    const items = await listUserData(guildId);
    let totalMessages = 0;
    let totalUsers = 0;
    for (const it of items) {
      if (it.userData && typeof it.userData.messages === 'number') {
        totalMessages += it.userData.messages;
        totalUsers++;
      }
    }
    if (!totalUsers) return;
    const avg = (totalMessages / totalUsers).toFixed(2);
    const g = client.guilds.cache.get(guildId);
    if (!g) return;
    const repChanId = c.reportSettings.weeklyReportChannel;
    const repChan = g.channels.cache.get(repChanId);
    if (!repChan?.isTextBased()) return;
    const rep = `**Weekly Report for ${g.name}**\n\n- Total Messages: ${totalMessages}\n- Active Users: ${totalUsers}\n- Avg Msg/User: ${avg}`;
    await repChan.send(rep);
  } catch (err) {
    console.error(`generateWeeklyReport error for ${guildId}: ${err}`);
  }
}

async function generateMonthlyReport(guildId) {
  try {
    const c = await getConfig(guildId);
    if (!c) return;
    const items = await listUserData(guildId);
    let totalMessages = 0;
    let totalUsers = 0;
    for (const it of items) {
      if (it.userData && typeof it.userData.messages === 'number') {
        totalMessages += it.userData.messages;
        totalUsers++;
      }
    }
    if (!totalUsers) return;
    const avg = (totalMessages / totalUsers).toFixed(2);
    const g = client.guilds.cache.get(guildId);
    if (!g) return;
    const repChanId = c.reportSettings.monthlyReportChannel;
    const repChan = g.channels.cache.get(repChanId);
    if (!repChan?.isTextBased()) return;
    const rep = `**Monthly Report for ${g.name}**\n\n- Total Messages: ${totalMessages}\n- Active Users: ${totalUsers}\n- Avg Msg/User: ${avg}`;
    await repChan.send(rep);
  } catch (err) {
    console.error(`generateMonthlyReport error for ${guildId}: ${err}`);
  }
}

client.on('guildMemberRemove', async () => {});

client.login(token).catch(console.error);
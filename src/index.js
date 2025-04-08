require('dotenv').config();

console.log("LOADED PERSONAL_KEY_ID:", process.env.PERSONAL_AWS_ACCESS_KEY_ID ? 'Exists' : 'MISSING');
console.log("LOADED PERSONAL_SECRET:", process.env.PERSONAL_AWS_SECRET_ACCESS_KEY ? 'Exists' : 'MISSING');

const { Client, GatewayIntentBits, Partials, Collection, REST, Routes, AttachmentBuilder, EmbedBuilder } = require('discord.js');
const interactionHandler = require('./interactionHandler');
const canvafy = require('canvafy');
const cron = require('node-cron');
const path = require('path');
const fs = require('fs');
const {
  getUserData,
  getRawUserData,
  saveUserData,
  listUserData,
  updateUserData,
  incrementMessageLeaderWins,
  queryLeaderboard
} = require('./dynamoDB');
const { getConfig, saveConfig, ensureConfigStructure } = require('./configManager');

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
    if (command.data && command.data.name) {
      client.commands.set(command.data.name, command);
      commands.push(command.data.toJSON());
    } else {
      console.warn(`Command file ${file} is missing 'data' or 'data.name' property and was not loaded.`);
    }
  } catch (error) {
    console.error(`Failed to load command ${file}:`, error);
  }
}


const streakCooldowns = new Map();
const userMessageData = {};

// -------------------------
// UTILITY FUNCTIONS
// -------------------------
function getSimilarityScore(text1, text2) {
  if (!text1 || !text2) return 0;
  const [shorter, longer] = text1.length < text2.length ? [text1, text2] : [text2, text1];
  if (longer.length === 0) return 1.0;
  const editDistance = levenshteinDistance(shorter, longer);
  return (longer.length - editDistance) / longer.length;
}

function levenshteinDistance(a, b) {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;
  const matrix = Array(b.length + 1).fill(null).map(() => Array(a.length + 1).fill(null));
  for (let i = 0; i <= a.length; i += 1) { matrix[0][i] = i; }
  for (let j = 0; j <= b.length; j += 1) { matrix[j][0] = j; }
  for (let j = 1; j <= b.length; j += 1) {
    for (let i = 1; i <= a.length; i += 1) {
      const indicator = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[j][i] = Math.min(
          matrix[j][i - 1] + 1,
          matrix[j - 1][i] + 1,
          matrix[j - 1][i - 1] + indicator,
      );
    }
  }
  return matrix[b.length][a.length];
}


function detectSpam(message, userId) {
  const currentTime = Date.now();
  const data = userMessageData[userId] || { lastMessageContent: null, lastTime: 0 };
  const timeDifference = currentTime - data.lastTime;
  const similarityScore = data.lastMessageContent ? getSimilarityScore(data.lastMessageContent, message.content) : 0;

  if (timeDifference < 2000 && message.content.length > 5 && data.lastMessageContent === message.content) {
    console.log(`[SPAM DETECTION] User ${userId} sent exact same message rapidly.`);
    return true;
  }
  if (timeDifference < 3000 && similarityScore > 0.90 && message.content.length > 10) {
    console.log(`[SPAM DETECTION] User ${userId} sent highly similar message rapidly.`);
    return true;
  }

  userMessageData[userId] = { lastMessageContent: message.content, lastTime: currentTime };
  return false;
}

function setLongTimeout(callback, duration) {
  const maxDuration = 2147483647;
  if (duration <= 0) {
    callback();
  } else if (duration > maxDuration) {
    setTimeout(() => {
      setLongTimeout(callback, duration - maxDuration);
    }, maxDuration);
  } else {
    setTimeout(callback, duration);
  }
}

// -------------------------
// GUILD CONFIG INITIALIZATION
// -------------------------
async function initializeGuildConfig(guildId) {
  let config = await getConfig(guildId);
  if (!config) {
    console.log(`Initializing default config for guild ${guildId}`);
    const initialConfig = {};
    ensureConfigStructure(initialConfig);
    try {
      await saveConfig(guildId, initialConfig);
      return { config: initialConfig, newlyCreated: true };
    } catch (saveError) {
      console.error(`Failed to save initial config for guild ${guildId}:`, saveError);
      return { config: null, newlyCreated: false };
    }
  }
  ensureConfigStructure(config);
  return { config, newlyCreated: false };
}

// -------------------------
// CLIENT EVENTS
// -------------------------
client.on('ready', async () => {
  console.log(`Logged in as ${client.user.tag}!`);

  const rest = new REST({ version: '10' }).setToken(token);
  try {
    console.log(`Started refreshing ${commands.length} application (/) commands.`);
    await rest.put(
        Routes.applicationCommands(clientId),
        { body: commands },
    );
    console.log(`Successfully reloaded ${commands.length} application (/) commands.`);
  } catch (error) {
    console.error('Error registering application commands:', error);
  }

  try {
    const guilds = client.guilds.cache.map(g => g.id);
    console.log(`Initializing configurations for ${guilds.length} guilds...`);
    for (const gId of guilds) {
      const guild = client.guilds.cache.get(gId);
      if (!guild) {
        console.warn(`Could not find guild ${gId} in cache during init.`);
        continue;
      }
      try {
        const { config, newlyCreated } = await initializeGuildConfig(gId);
        if (config && newlyCreated) {
          await sendConfigMessage(guild);
        }
      } catch (initError) {
        console.error(`Error initializing config for guild ${guild.name} (${gId}):`, initError);
      }
    }
    console.log("Guild configuration initialization complete.");
  } catch (error) {
    console.error(`Error during guild initialization loop: ${error.message}`);
  }

  console.log("Setting up scheduled tasks...");
  scheduleDailyReset();
  scheduleMessageLeaderAnnounce();
  scheduleWeeklyReport();
  scheduleMonthlyReport();
  console.log("Scheduled tasks set up.");
});

client.on('interactionCreate', async interaction => {
  try {
    await interactionHandler(client, interaction);
  } catch (error) {
    console.error(`Error handling interaction (ID: ${interaction.id}, Type: ${interaction.type}):`, error);
    if (interaction.isRepliable()) {
      const errorMessage = { content: 'An unexpected error occurred while processing your request. Please try again later.', ephemeral: true };
      try {
        if (interaction.deferred || interaction.replied) {
          await interaction.followUp(errorMessage);
        } else {
          await interaction.reply(errorMessage);
        }
      } catch (replyError) {
        console.error("Failed to send interaction error reply:", replyError);
      }
    }
  }
});

client.on('guildCreate', async guild => {
  console.log(`Joined new guild: ${guild.name} (${guild.id})`);
  try {
    const { config, newlyCreated } = await initializeGuildConfig(guild.id);
    if (config && newlyCreated) {
      console.log(`Initialized new config for ${guild.name}. Sending setup message.`);
      await sendConfigMessage(guild);
    } else if (!config) {
      console.error(`Failed to initialize config upon joining guild ${guild.name} (${guild.id})`);
    } else {
      console.log(`Configuration already existed for ${guild.name}.`);
    }
  } catch (error) {
    console.error(`Error during guildCreate initialization for ${guild.name} (${guild.id}):`, error);
  }
});


client.on('messageCreate', async message => {
  if (!message.guild || message.author.bot) return;
  if (message.channel.name.toLowerCase().includes('verification')) return;

  if (detectSpam(message, message.author.id)) {
    return;
  }

  try {
    await handleUserMessage(message.guild.id, message.author.id, message.channel, message);
  } catch (error) {
    console.error(`Error in handleUserMessage call for ${message.author.id} in guild ${message.guild.id}:`, error);
  }
});


// -------------------------
// SCHEDULED TASKS
// -------------------------`
function scheduleDailyReset() {
  // PRODUCTION: '0 0 * * *' (Midnight UTC)
  // TESTING :
  const cronPattern = '0 0 * * *';
  console.log(`Daily Reset scheduled with pattern: ${cronPattern}`);
  cron.schedule(cronPattern, () => {
    console.log('Running daily reset task (streaks, thresholds)...');
    resetDailyStreaks().catch(error => {
      console.error('Error during scheduled daily reset:', error);
    });
  }, { scheduled: true, timezone: "UTC" });
}

function scheduleMessageLeaderAnnounce() {
  // PRODUCTION: '0 18 * * 0' (Sunday 18:00 UTC)
  // TESTING :
  const cronPattern = '0 18 * * 0';
  console.log(`Weekly Message Leader announcement scheduled with pattern: ${cronPattern}`);
  cron.schedule(cronPattern, () => {
    console.log('Running weekly message leader announcement...');
    announceMessageLeaders().catch(error => {
      console.error('Error during scheduled message leader announcement:', error);
    });
  }, { scheduled: true, timezone: "UTC" });
}

function scheduleWeeklyReport() {
  // PRODUCTION: '5 18 * * 0' (Sunday 18:05 UTC)
  // TESTING :
  const cronPattern = '5 18 * * 0';
  console.log(`Weekly Report generation scheduled with pattern: ${cronPattern}`);
  cron.schedule(cronPattern, async () => {
    console.log('Running weekly report generation...');
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      try {
        await generateWeeklyReport(gId);
      } catch (error) {
        console.error(`Error generating weekly report for guild ${gId}:`, error);
      }
    }
  }, { scheduled: true, timezone: "UTC" });
}

function scheduleMonthlyReport() {
  // PRODUCTION: '10 0 1 * *' (1st of Month 00:10 UTC)
  // TESTING
  const cronPattern = '10 0 1 * *';
  console.log(`Monthly Report generation scheduled for 1st of month at 00:10 UTC.`);
  cron.schedule(cronPattern, async () => {
    console.log('Running monthly report generation...');
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      try {
        await generateMonthlyReport(gId);
      } catch (error) {
        console.error(`Error generating monthly report for guild ${gId}:`, error);
      }
    }
  }, { scheduled: true, timezone: "UTC" });
}

// -------------------------
// ANNOUNCEMENT AND REPORT FUNCTIONS
// -------------------------
async function announceMessageLeaders() {
  const guilds = client.guilds.cache.map(g => g.id);
  for (const gId of guilds) {
    const currentGuild = client.guilds.cache.get(gId);
    if (!currentGuild) continue;
    const config = await getConfig(gId);
    if (!config?.messageLeaderSystem?.enabled || !config.messageLeaderSystem.channelMessageLeader) continue;
    try {
      const leaderboardItems = await queryLeaderboard('messages', gId, 10);
      if (!leaderboardItems || leaderboardItems.length === 0) continue;
      await currentGuild.members.fetch();
      const topUsersData = leaderboardItems.map(item => {
        const member = currentGuild.members.cache.get(item.discordAccountId);
        if (!member) return null;
        return { userId: item.discordAccountId, username: member.user.username, tag: member.user.tag, avatar: member.user.displayAvatarURL({ format: 'png', dynamic: true, size: 128 }), score: item.count };
      }).filter(item => item !== null);
      if (topUsersData.length === 0) continue;
      const canvafyUsers = topUsersData.map((user, index) => ({ top: index + 1, avatar: user.avatar, tag: user.username, score: user.score }));
      const imageBuffer = await new canvafy.Top().setOpacity(0.7).setScoreMessage("Messages:").setBackground('image', 'https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg').setColors({ box: '#212121', username: '#ffffff', score: '#ffffff', firstRank: '#f7c716', secondRank: '#9e9e9e', thirdRank: '#94610f' }).setUsersData(canvafyUsers).build();
      const attachment = new AttachmentBuilder(imageBuffer, { name: `message-leaderboard-${gId}.png` });
      let msgContent = `🎉 **Message Leaders for the week in ${currentGuild.name}!** 🔥\n\n`;
      topUsersData.slice(0, 3).forEach((user, index) => { const medal = ['🏆', '🥈', '🥉'][index]; msgContent += `${medal} **${index+1}${index === 0 ? 'st' : index === 1 ? 'nd' : 'rd'} Place:** <@${user.userId}> (${user.username}) - ${user.score} messages\n`; });
      msgContent += `\nKeep up the great engagement!`;
      const announcementChannel = currentGuild.channels.cache.get(config.messageLeaderSystem.channelMessageLeader);
      if (!announcementChannel?.isTextBased()) continue;
      await announcementChannel.send({ content: msgContent, files: [attachment] }).catch(e => console.error(`Failed leader announce ${gId}: ${e.message}`));
      const leaderRoleId = config.messageLeaderSystem.roleMessageLeader;
      if (leaderRoleId) {
        const leaderRole = currentGuild.roles.cache.get(leaderRoleId);
        if (leaderRole) {
          for (const member of leaderRole.members.values()) { if (!topUsersData.slice(0, 1).some(w => w.userId === member.id)) await member.roles.remove(leaderRole, 'End term').catch(e => console.error(`Role remove fail ${member.id}: ${e.message}`)); }
          const winnerMember = currentGuild.members.cache.get(topUsersData[0].userId);
          if (winnerMember && !winnerMember.roles.cache.has(leaderRole.id)) await winnerMember.roles.add(leaderRole, 'Weekly Leader').catch(e => console.error(`Role add fail ${winnerMember.id}: ${e.message}`));
        }
      }
      await incrementMessageLeaderWins(gId, topUsersData[0].userId).catch(e => console.error(`Win increment fail ${topUsersData[0].userId}: ${e.message}`));
      const allUserData = await listUserData(gId);
      const resetPromises = allUserData.map(({ userId }) => updateUserData(gId, userId, { messages: 0 }).catch(err => console.error(`Msg reset fail ${userId}:`, err)));
      await Promise.all(resetPromises);
    } catch (error) { console.error(`Error processing leaders guild ${gId}:`, error); }
  }
}


async function assignRole(guildId, userId, roleId, reason = 'Automated role assignment') {
  const guild = client.guilds.cache.get(guildId);
  if (!guild) { console.warn(`[assignRole] Guild ${guildId} not found.`); return; }
  const role = guild.roles.cache.get(roleId);
  if (!role) { console.warn(`[assignRole] Role ${roleId} not found.`); return; }
  try {
    const member = await guild.members.fetch(userId).catch(()=>null);
    if (!member) return;
    if (guild.members.me && role.position >= guild.members.me.roles.highest.position) {
      console.warn(`[assignRole] Cannot assign role ${role.name} (${roleId}) - hierarchy issue in ${guild.name}.`); return;
    }
    if (!member.roles.cache.has(role.id)) await member.roles.add(role, reason);
  } catch (error) {
    if (error.code === 50013) console.error(`[assignRole] Missing Permissions for role ${role.name} in ${guild.name}.`);
    else if (error.code !== 10007 && error.code !== 10013) console.error(`[assignRole] Error assigning role ${roleId} to ${userId}:`, error);
  }
}

async function removeRole(guildId, userId, roleId, reason = 'Automated role removal') {
  const guild = client.guilds.cache.get(guildId);
  if (!guild) return;
  const role = guild.roles.cache.get(roleId);
  if (!role) return;
  try {
    const member = await guild.members.fetch(userId).catch(()=>null);
    if (!member) return;
    if (guild.members.me && role.position >= guild.members.me.roles.highest.position) {
      console.warn(`[removeRole] Cannot remove role ${role.name} (${roleId}) - hierarchy issue in ${guild.name}.`); return;
    }
    if (member.roles.cache.has(role.id)) await member.roles.remove(role, reason);
  } catch (error) {
    if (error.code === 50013) console.error(`[removeRole] Missing Permissions for role ${role.name} in ${guild.name}.`);
    else if (error.code !== 10007 && error.code !== 10013) console.error(`[removeRole] Error removing role ${roleId} from ${userId}:`, error);
  }
}

async function resetDailyStreaks() {
  console.log("Starting daily reset process...");
  const guilds = client.guilds.cache.map(g => g.id);
  const today = new Date().toISOString().split('T')[0];

  for (const gId of guilds) {
    const currentGuild = client.guilds.cache.get(gId);
    if (!currentGuild) continue;
    const config = await getConfig(gId);
    if (!config) continue;

    const streakEnabled = config.streakSystem?.enabled;
    const baseThreshold = config.streakSystem?.streakThreshold ?? 10;

    try {
      const usersData = await listUserData(gId);
      const updatePromises = [];

      for (const { userId, userData } of usersData) {
        const userUpdates = {};
        let userLostStreak = false;
        const oldStreak = userData.streak || 0;

        if (userData.receivedDaily) userUpdates.receivedDaily = false;
        if (streakEnabled && userData.threshold !== baseThreshold) userUpdates.threshold = baseThreshold;

        if (streakEnabled && oldStreak > 0 && !userData.receivedDaily) {
          // FOR TESTING : console.log(`User ${userId} streak reset from ${oldStreak} in ${gId}.`);
          userUpdates.streak = 0;
          userUpdates.lastStreakLoss = new Date().toISOString();
          userLostStreak = true;
        }

        const trackedDays = (userData.daysTracked || 0) + 1;
        userUpdates.daysTracked = trackedDays;

        const isActiveToday = userData.messageHeatmap?.some(entry => entry.date === today && entry.messages > 0) ?? false;
        const consecutiveInactive = isActiveToday ? 0 : (userData.consecutiveInactiveDays || 0) + 1;
        // userUpdates.consecutiveInactiveDays = consecutiveInactive; // Can uncomment if we want to track this

        if (isActiveToday) { userUpdates.activeDaysCount = (userData.activeDaysCount || 0) + 1; }
        if (consecutiveInactive > (userData.longestInactivePeriod || 0)) { userUpdates.longestInactivePeriod = consecutiveInactive; }
        if (trackedDays > 0) { userUpdates.averageMessagesPerDay = parseFloat(((userData.totalMessages || 0) / trackedDays).toFixed(4)); }

        if (Object.keys(userUpdates).length > 0) {
          updatePromises.push(
              updateUserData(gId, userId, userUpdates)
                  .then(() => { if (userLostStreak && streakEnabled) return removeStreakRoles(gId, userId, config, oldStreak); })
                  .catch(err => console.error(`Failed daily update for ${userId} in ${gId}:`, err))
          );
        }
      }
      await Promise.all(updatePromises);
      console.log(`Finished daily reset for guild ${gId}.`);
    } catch (error) { console.error(`Error during daily reset for guild ${gId}:`, error); }
  }
  console.log("Daily reset cycle finished.");
}

async function removeStreakRoles(guildId, userId, config, oldStreakValue) {
  if (!config?.streakSystem?.enabled || oldStreakValue <= 0) return;
  const guild = client.guilds.cache.get(guildId);
  if (!guild) return;
  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) return;
  const rolesToRemove = [];
  for (const key in config.streakSystem) {
    if (key.startsWith('role') && key.endsWith('day')) {
      const days = parseInt(key.replace('role', '').replace('day', ''), 10);
      if (!isNaN(days) && oldStreakValue >= days) {
        const roleId = config.streakSystem[key];
        const role = guild.roles.cache.get(roleId);
        if (role && member.roles.cache.has(roleId)) {
          if (guild.members.me && role.position < guild.members.me.roles.highest.position) rolesToRemove.push(roleId);
          else console.warn(`[removeStreakRoles] Cannot remove role ${role.name} (${roleId}) - hierarchy issue.`);
        }
      }
    }
  }
  if (rolesToRemove.length > 0) {
    try {
      await member.roles.remove(rolesToRemove, `Lost ${oldStreakValue}-day streak`);
      await member.send(`You lost your **${oldStreakValue}-day** message streak in **${guild.name}**...`).catch(()=>{/*ignore DM fail*/});
    } catch (error) {
      if (error.code === 50013) console.error(`[removeStreakRoles] Missing Permissions in ${guild.name}.`);
      else console.error(`[removeStreakRoles] Error removing roles from ${userId}:`, error);
    }
  }
}

async function generateWeeklyReport(guildId) {
  const currentGuild = client.guilds.cache.get(guildId);
  if (!currentGuild) return;
  const config = await getConfig(guildId);
  if (!config?.reportSettings?.weeklyReportChannel) return;
  const reportChannel = currentGuild.channels.cache.get(config.reportSettings.weeklyReportChannel);
  if (!reportChannel?.isTextBased()) return;
  try {
    const usersData = await listUserData(guildId);
    if (!usersData || usersData.length === 0) { await reportChannel.send(`**Weekly Report - ${currentGuild.name}**: No user data.`); return; }
    let totalMsg = 0, activeUsers = 0, totalStr = 0, usersStr = 0, highestStr = 0, highestLvl = 0;
    usersData.forEach(({ userData: ud }) => {
      const weeklyMsgs = ud.messages || 0; totalMsg += weeklyMsgs; if (weeklyMsgs > 0) activeUsers++;
      const currentStreak = ud.streak || 0; if (currentStreak > 0) { totalStr += currentStreak; usersStr++; }
      highestStr = Math.max(highestStr, ud.highestStreak || 0); highestLvl = Math.max(highestLvl, ud.experience?.level || 0);
    });
    const avgMsg = activeUsers > 0 ? (totalMsg / activeUsers).toFixed(2) : '0.00';
    const avgStr = usersStr > 0 ? (totalStr / usersStr).toFixed(2) : '0.00';
    const embed = new EmbedBuilder().setColor('#3498DB').setTitle(`Weekly Activity Report - ${currentGuild.name}`)
        .setDescription(`Summary ending ${new Date().toLocaleDateString()}`)
        .addFields(
            { name: 'Msgs Sent', value: `${totalMsg}`, inline: true }, { name: 'Active Users', value: `${activeUsers}`, inline: true },
            { name: 'Avg Msgs/User', value: `${avgMsg}`, inline: true }, { name: 'Highest Streak', value: `${highestStr}`, inline: true },
            { name: 'Users w/ Streaks', value: `${usersStr}`, inline: true }, { name: 'Avg Streak', value: `${avgStr}`, inline: true },
            { name: 'Highest Level', value: `${highestLvl}`, inline: true }
        ).setTimestamp().setFooter({ text: `Guild ID: ${guildId}` });
    const topMsgrs = await queryLeaderboard('messages', guildId, 3);
    if (topMsgrs.length > 0) { embed.addFields({ name: 'Top Messagers', value: topMsgrs.map((i,idx)=>`${idx+1}. <@${i.discordAccountId}> (${i.count})`).join('\n'), inline: false }); }
    await reportChannel.send({ embeds: [embed] });
  } catch (err) { console.error(`Gen Weekly Report Err (${guildId}): ${err}`); }
}

async function generateMonthlyReport(guildId) {
  const currentGuild = client.guilds.cache.get(guildId);
  if (!currentGuild) return;
  const config = await getConfig(guildId);
  if (!config?.reportSettings?.monthlyReportChannel) return;
  const reportChannel = currentGuild.channels.cache.get(config.reportSettings.monthlyReportChannel);
  if (!reportChannel?.isTextBased()) return;
  try {
    const usersData = await listUserData(guildId);
    if (!usersData || usersData.length === 0) { await reportChannel.send(`**Monthly Report - ${currentGuild.name}**: No user data.`); return; }
    let totalLifeMsg = 0, totalUsers = usersData.length, highestStr = 0, highestLvl = 0, totalWins = 0;
    usersData.forEach(({ userData: ud }) => {
      totalLifeMsg += ud.totalMessages || 0; highestStr = Math.max(highestStr, ud.highestStreak || 0);
      highestLvl = Math.max(highestLvl, ud.experience?.level || 0); totalWins += ud.messageLeaderWins || 0;
    });
    const avgLifeMsg = totalUsers > 0 ? (totalLifeMsg / totalUsers).toFixed(2) : '0.00';
    const embed = new EmbedBuilder().setColor('#9B59B6').setTitle(`Monthly Report - ${currentGuild.name}`)
        .setDescription(`Summary for ${new Date().toLocaleDateString('default', { month: 'long', year: 'numeric' })}`)
        .addFields(
            { name: 'Lifetime Msgs', value: `${totalLifeMsg}`, inline: true }, { name: 'Total Users', value: `${totalUsers}`, inline: true },
            { name: 'Avg Lifetime Msgs', value: `${avgLifeMsg}`, inline: true }, { name: 'All-Time High Streak', value: `${highestStr}`, inline: true },
            { name: 'All-Time High Lvl', value: `${highestLvl}`, inline: true }, { name: 'Total Leader Wins', value: `${totalWins}`, inline: true }
        ).setTimestamp().setFooter({ text: `Guild ID: ${guildId}` });
    const topStreakers = await queryLeaderboard('highestStreak', guildId, 3);
    if (topStreakers.length > 0) { embed.addFields({ name: 'Top Highest Streaks', value: topStreakers.map((i,idx)=>`${idx+1}. <@${i.discordAccountId}> (${i.count})`).join('\n'), inline: false }); }
    await reportChannel.send({ embeds: [embed] });
  } catch (err) { console.error(`Gen Monthly Report Err (${guildId}): ${err}`); }
}

async function sendConfigMessage(guild) {
  try {
    let channelToSend = guild.systemChannel ||
        guild.publicUpdatesChannel ||
        guild.channels.cache.find(ch => ch.type === 0 && ch.permissionsFor(guild.members.me).has('SendMessages'));
    if (channelToSend) {
      let adder = null;
      try { if (guild.members.me?.permissions.has('ViewAuditLog')) { const logs = await guild.fetchAuditLogs({ type: 28, limit: 1 }); const entry = logs.entries.first(); if (entry && (Date.now() - entry.createdTimestamp < 300000)) adder = entry.executor; } } catch {}
      let msgContent = `Hello! Thanks for adding me to **${guild.name}**!`;
      if (adder) msgContent = `Hello ${adder}! Thanks for adding me to **${guild.name}**!`;
      msgContent += `\n\nUse \`/setup-bot\` or \`/stitches-configuration\` to configure features.`;
      await channelToSend.send(msgContent);
    } else console.warn(`Could not find suitable channel in guild ${guild.id}`);
  } catch (err) { console.error(`Error sending config message in guild ${guild.id}: ${err}`); }
}


// -------------------------
// MESSAGE HANDLING
// -------------------------
async function handleUserMessage(guildId, userId, channel, message) {
  const now = Date.now();
  const cooldownTime = 1000;
  const lastProcessed = streakCooldowns.get(userId) || 0;
  if (now - lastProcessed < cooldownTime) return;
  streakCooldowns.set(userId, now);

  try {
    const config = await getConfig(guildId);
    if (!config) return;
    const currentGuild = client.guilds.cache.get(guildId);
    if (!currentGuild) return;


    const rawUserData = await getRawUserData(userId);
    let userRec;
    let needsMigrationSave = false;

    if (!rawUserData) {
      // FOR TESTING : console.log(`[Migrate] New user detected: ${userId}. Initializing data.`);
      userRec = {
        streak: 0, highestStreak: 0, messages: 0,
        threshold: config.streakSystem?.streakThreshold ?? 10,
        receivedDaily: false, messageLeaderWins: 0, highestMessageCount: 0,
        mostConsecutiveLeader: 0, totalMessages: 0, daysTracked: 0,
        averageMessagesPerDay: 0, activeDaysCount: 0, longestInactivePeriod: 0,
        lastStreakLoss: null, messageHeatmap: [], milestones: [], rolesAchieved: [],
        experience: { totalXp: 0, level: 0 }, boosters: 1,
        lastMessage: { time: 0, content: '', date: null },
        channelsParticipated: [], mentionsRepliesCount: { mentions: 0, replies: 0 }
      };
      needsMigrationSave = true;
    } else if (rawUserData.userData && typeof rawUserData.userData === 'object') {

      // FOR TESTING : console.log(`[Migrate] User ${userId} already in new format.`);
      userRec = rawUserData.userData;
      if (!userRec.experience) userRec.experience = { totalXp: 0, level: 0 };
      if (!userRec.lastMessage) userRec.lastMessage = { time: 0, content: '', date: null };
      if (!userRec.mentionsRepliesCount) userRec.mentionsRepliesCount = { mentions: 0, replies: 0 };
    } else {
      console.log(`[Migrate] Old data format detected for user ${userId}. Migrating...`);
      needsMigrationSave = true;
      const oldData = rawUserData;

      userRec = {
        streak: oldData.streak ?? 0,
        highestStreak: oldData.highestStreak ?? oldData.streak ?? 0,
        messages: oldData.messages ?? 0,
        threshold: oldData.threshold ?? config.streakSystem?.streakThreshold ?? 10,
        receivedDaily: oldData.receivedDaily ?? false,
        messageLeaderWins: oldData.messageLeaderWins ?? 0,
        highestMessageCount: oldData.highestMessageCount ?? 0,
        mostConsecutiveLeader: oldData.mostConsecutiveLeader ?? 0,
        totalMessages: oldData.totalMessages ?? oldData.messages ?? 0,
        daysTracked: oldData.daysTracked ?? 0,
        averageMessagesPerDay: oldData.averageMessagesPerDay ?? 0,
        activeDaysCount: oldData.activeDaysCount ?? 0,
        longestInactivePeriod: oldData.longestInactivePeriod ?? 0,
        lastStreakLoss: oldData.lastStreakLoss ?? null,
        messageHeatmap: Array.isArray(oldData.messageHeatmap) ? oldData.messageHeatmap : [],
        milestones: Array.isArray(oldData.milestones) ? oldData.milestones : [],
        rolesAchieved: Array.isArray(oldData.rolesAchieved) ? oldData.rolesAchieved : [],
        experience: {
          totalXp: oldData.experience?.totalXp ?? oldData.totalXp ?? oldData.experience ?? 0,
          level: oldData.experience?.level ?? oldData.level ?? 0
        },
        boosters: oldData.boosters ?? 1,
        lastMessage: {
          time: oldData.lastMessage?.time ?? oldData.lastMessageTime ?? 0,
          content: oldData.lastMessage?.content ?? '',
          date: oldData.lastMessage?.date ?? null
        },
        channelsParticipated: Array.isArray(oldData.channelsParticipated) ? oldData.channelsParticipated : [],
        mentionsRepliesCount: {
          mentions: oldData.mentionsRepliesCount?.mentions ?? oldData.mentionsCount ?? 0,
          replies: oldData.mentionsRepliesCount?.replies ?? oldData.repliesCount ?? 0
        }
      };
      if (!userRec.experience) userRec.experience = { totalXp: 0, level: 0 };
      if (!userRec.lastMessage) userRec.lastMessage = { time: 0, content: '', date: null };
      if (!userRec.mentionsRepliesCount) userRec.mentionsRepliesCount = { mentions: 0, replies: 0 };
      console.log(`[Migrate] Finished mapping old data for user ${userId}.`);
    }


    const updates = {};
    let needsUpdateSave = false;
    const todayISO = new Date().toISOString().split('T')[0];

    updates.lastMessage = { time: now, content: message.content.substring(0, 200), date: todayISO };
    userRec.totalMessages = (userRec.totalMessages || 0) + 1; updates.totalMessages = userRec.totalMessages;
    userRec.messages = (userRec.messages || 0) + 1; updates.messages = userRec.messages;
    needsUpdateSave = true;

    if (!userRec.channelsParticipated?.includes(channel.id)) {
      const currentChannels = Array.isArray(userRec.channelsParticipated) ? [...userRec.channelsParticipated] : [];
      currentChannels.push(channel.id);
      userRec.channelsParticipated = currentChannels.slice(-20);
      updates.channelsParticipated = userRec.channelsParticipated;
    }

    const currentHeatmap = Array.isArray(userRec.messageHeatmap) ? [...userRec.messageHeatmap] : [];
    let todayEntry = currentHeatmap.find(entry => entry.date === todayISO);
    if (todayEntry) { todayEntry.messages += 1; } else { currentHeatmap.push({ date: todayISO, messages: 1 }); }
    if (currentHeatmap.length > 60) {
      currentHeatmap.sort((a,b)=>new Date(b.date)-new Date(a.date));
      userRec.messageHeatmap = currentHeatmap.slice(0, 60);
    } else {
      userRec.messageHeatmap = currentHeatmap;
    }
    updates.messageHeatmap = userRec.messageHeatmap;

    let streakIncreasedToday = false, milestoneAchieved = null, milestoneRoleId = null;
    let currentStreakValue = userRec.streak || 0;
    if (config.streakSystem?.enabled && !userRec.receivedDaily) {
      let currentThreshold = userRec.threshold ?? (config.streakSystem.streakThreshold ?? 10);
      if (currentThreshold > 0) { currentThreshold -= 1; userRec.threshold = currentThreshold; updates.threshold = currentThreshold; }
      if (currentThreshold === 0) {
        currentStreakValue = (userRec.streak || 0) + 1;
        userRec.streak = currentStreakValue; updates.streak = currentStreakValue;
        userRec.receivedDaily = true; updates.receivedDaily = true;
        streakIncreasedToday = true; needsUpdateSave = true;
        if (currentStreakValue > (userRec.highestStreak || 0)) { userRec.highestStreak = currentStreakValue; updates.highestStreak = currentStreakValue; }
        for (const key in config.streakSystem) {
          if (key.startsWith('role') && key.endsWith('day')) {
            const days = parseInt(key.replace('role','').replace('day',''), 10);
            if (!isNaN(days) && currentStreakValue === days) {
              milestoneAchieved = days; milestoneRoleId = config.streakSystem[key];
              const currentM = Array.isArray(userRec.milestones) ? [...userRec.milestones] : []; currentM.push({ milestone: days, date: new Date().toISOString() }); userRec.milestones=currentM; updates.milestones=currentM;
              const currentR = Array.isArray(userRec.rolesAchieved) ? [...userRec.rolesAchieved] : []; if(!currentR.includes(milestoneRoleId)){ currentR.push(milestoneRoleId); userRec.rolesAchieved=currentR; updates.rolesAchieved=currentR; }
              await assignRole(guildId, userId, milestoneRoleId, `${days}-Day Streak`); break;
            }
          }
        }
      }
    }

    let levelIncreasedToday = false; let currentLevelValue = userRec.experience?.level || 0;
    if (config.levelSystem?.enabled) {
      const xpGain = Math.floor((config.levelSystem.xpPerMessage || 10) * (userRec.boosters || 1));
      if (xpGain > 0) {
        let currentXp = userRec.experience?.totalXp || 0; currentXp += xpGain;
        const baseXP = 100; const multiplier = config.levelSystem.levelMultiplier || 1.5;
        let xpNeeded = Math.floor(baseXP * Math.pow(multiplier, currentLevelValue));
        while (currentXp >= xpNeeded && currentLevelValue < 100) { currentXp -= xpNeeded; currentLevelValue++; levelIncreasedToday = true; xpNeeded = Math.floor(baseXP * Math.pow(multiplier, currentLevelValue)); }
        userRec.experience.totalXp = currentXp; updates['experience.totalXp'] = currentXp;
        if (levelIncreasedToday) {
          userRec.experience.level = currentLevelValue; updates['experience.level'] = currentLevelValue;
          const roleKey = `roleLevel${currentLevelValue}`; const roleId = config.levelSystem[roleKey];
          if (roleId) { const currentR = Array.isArray(userRec.rolesAchieved)?[...userRec.rolesAchieved]:[]; if(!currentR.includes(roleId)){ currentR.push(roleId); userRec.rolesAchieved=currentR; updates.rolesAchieved=currentR; } await assignRole(guildId, userId, roleId, `Lvl ${currentLevelValue}`); }
        }
        needsUpdateSave = true;
      }
    }


    if (needsMigrationSave) {
      console.log(`[Migrate] Saving migrated data structure for user ${userId}.`);
      await saveUserData(guildId, userId, userRec);
    } else if (needsUpdateSave) {
      // FOR TESTING console.log(`[Migrate] Updating existing new format data for user ${userId}.`);
      await updateUserData(guildId, userId, updates);
    }


    if (streakIncreasedToday && config.streakSystem?.enabled) {
      const streakChId = config.streakSystem.channelStreakOutput; let streakCh = streakChId ? currentGuild.channels.cache.get(streakChId) : null; if (!streakCh?.isTextBased()) streakCh = channel;
      if (streakCh?.isTextBased()) {
        try {
          const img = await new canvafy.LevelUp().setAvatar(message.author.displayAvatarURL({format:'png',size:128})).setBackground("image","https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg").setUsername(message.author.username).setBorder("#FF0000").setAvatarBorder("#FFFFFF").setOverlayOpacity(0.7).setLevels(userRec.streak - 1, userRec.streak).build();
          const attach = new AttachmentBuilder(img, {name:`streak-${userId}.png`});
          let msg = `🎉 <@${userId}> has increased their message streak to **${userRec.streak}** days!`;
          if (milestoneAchieved && milestoneRoleId) { const role = currentGuild.roles.cache.get(milestoneRoleId); msg += `\nThey've earned the **${role ? role.name : `Role`}**!`; }
          await streakCh.send({ content: msg, files: [attach] });
        } catch (e) { console.error(`Failed streak announce for ${userId}:`, e); }
      }
    }

    if (levelIncreasedToday && config.levelSystem?.enabled && config.levelSystem.levelUpMessages) {
      const levelChId = config.levelSystem.channelLevelUp; let levelCh = levelChId ? currentGuild.channels.cache.get(levelChId) : null; if (!levelCh?.isTextBased()) levelCh = channel;
      if (levelCh?.isTextBased()) {
        try {
          let msg = `🎉 Congrats <@${userId}>! You reached **Level ${userRec.experience.level}**!`;
          const roleId = config.levelSystem[`roleLevel${userRec.experience.level}`];
          if (roleId) { const role = currentGuild.roles.cache.get(roleId); msg += ` You earned **${role ? role.name : `Role`}**!`; }
          await levelCh.send(msg);
        } catch (e) { console.error(`Failed level announce for ${userId}:`, e); }
      }
    }

  } catch (error) { console.error(`Unhandled error in handleUserMessage for user ${userId} guild ${guildId}:`, error); }
}

client.on('guildMemberRemove', async (member) => {
  console.log(`User ${member.user.tag} (${member.id}) left guild ${member.guild.name} (${member.guild.id})`);

});

client.login(token).catch(err => {
  console.error("Failed to login to Discord:", err);
  process.exit(1);
});
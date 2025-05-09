// Reported issues;
// - Streak roles not being added on milestone achievement.
// - Certain stats not being counted. Like active days, mentions, etc.

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
  saveUserData,
  listUserData,
  updateUserData,
  incrementMessageLeaderWins,
  queryLeaderboard,
  safeParseNumber
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

try {
  const eventFiles = fs.readdirSync('./events').filter(file => file.endsWith('.js'));
  for (const file of eventFiles) {
    const event = require(`./events/${file}`);
    if (!event.name || !event.execute) {
      console.error(`Error loading ${file}: Event does not properly export 'name' or 'execute'.`);
      continue;
    }
    if (event.once) {
      client.once(event.name, (...args) => event.execute(...args, client));
    } else {
      client.on(event.name, (...args) => event.execute(...args, client));
    }
  }
} catch (error) {
  console.error('Error reading event files:', error);
}

const streakCooldowns = new Map();
const userMessageData = {};


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



function scheduleDailyReset() {

  const cronPattern = '0 0 * * *';
  console.log(`Daily Reset scheduled with pattern: ${cronPattern}`);
  cron.schedule(cronPattern, () => {
    console.log(`[Cron ${cronPattern}] Triggered: Running daily reset task (streaks, thresholds)...`);
    resetDailyStreaks().catch(error => {
      console.error(`[Cron ${cronPattern}] Error during scheduled daily reset execution:`, error);
    });
  }, { scheduled: true, timezone: "UTC" });
}

function scheduleMessageLeaderAnnounce() {

  const cronPattern = '0 18 * * 0';
  console.log(`Weekly Message Leader announcement scheduled with pattern: ${cronPattern}`);
  cron.schedule(cronPattern, () => {
    console.log(`[Cron ${cronPattern}] Triggered: Running weekly message leader announcement...`);
    announceMessageLeaders().catch(error => {
      console.error(`[Cron ${cronPattern}] Error during scheduled message leader announcement execution:`, error);
    });
  }, { scheduled: true, timezone: "UTC" });
}

function scheduleWeeklyReport() {

  const cronPattern = '5 18 * * 0';
  console.log(`Weekly Report generation scheduled with pattern: ${cronPattern}`);
  cron.schedule(cronPattern, async () => {
    console.log(`[Cron ${cronPattern}] Triggered: Running weekly report generation...`);
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      try {
        await generateWeeklyReport(gId);
      } catch (error) {
        console.error(`[Cron ${cronPattern}] Error generating weekly report for guild ${gId}:`, error);
      }
    }
  }, { scheduled: true, timezone: "UTC" });
}

function scheduleMonthlyReport() {

  const cronPattern = '10 0 1 * *';
  console.log(`Monthly Report generation scheduled for 1st of month at 00:10 UTC.`);
  cron.schedule(cronPattern, async () => {
    console.log(`[Cron ${cronPattern}] Triggered: Running monthly report generation...`);
    const guilds = client.guilds.cache.map(g => g.id);
    for (const gId of guilds) {
      try {
        await generateMonthlyReport(gId);
      } catch (error) {
        console.error(`[Cron ${cronPattern}] Error generating monthly report for guild ${gId}:`, error);
      }
    }
  }, { scheduled: true, timezone: "UTC" });
}


async function announceMessageLeaders() {
  console.log("[Announce Leaders] Starting process for all guilds.");
  const guilds = client.guilds.cache.map(g => g.id);
  let processedGuilds = 0;
  let failedGuilds = 0;

  for (const gId of guilds) {
    const currentGuild = client.guilds.cache.get(gId);
    if (!currentGuild) {
      console.warn(`[Announce Leaders] Guild ${gId} not found in cache. Skipping.`);
      continue;
    }

    let config;
    try {
      config = await getConfig(gId);
      if (!config?.messageLeaderSystem?.enabled || !config.messageLeaderSystem.channelMessageLeader) {
        console.log(`[Announce Leaders] System disabled or channel not set for guild ${currentGuild.name} (${gId}). Skipping.`);
        continue;
      }
    } catch (configError) {
      console.error(`[Announce Leaders] Failed to get config for guild ${currentGuild.name} (${gId}):`, configError);
      failedGuilds++;
      continue;
    }

    console.log(`[Announce Leaders] Processing guild ${currentGuild.name} (${gId}).`);
    try {
      const leaderboardItems = await queryLeaderboard('messages', gId, 10);
      if (!leaderboardItems || leaderboardItems.length === 0) {
        console.log(`[Announce Leaders] No message leaderboard data found for guild ${currentGuild.name} (${gId}). Skipping announcement.`);
        continue;
      }

      await currentGuild.members.fetch();
      const topUsersData = leaderboardItems.map(item => {
        const member = currentGuild.members.cache.get(item.discordAccountId);
        if (!member) return null;
        return { userId: item.discordAccountId, username: member.user.username, tag: member.user.tag, avatar: member.user.displayAvatarURL({ format: 'png', dynamic: true, size: 128 }), score: item.count };
      }).filter(item => item !== null);

      if (topUsersData.length === 0) {
        console.log(`[Announce Leaders] No valid members found in leaderboard data for guild ${currentGuild.name} (${gId}). Skipping announcement.`);
        continue;
      }

      const canvafyUsers = topUsersData.map((user, index) => ({ top: index + 1, avatar: user.avatar, tag: user.username, score: user.score }));
      const imageBuffer = await new canvafy.Top().setOpacity(0.7).setScoreMessage("Messages:").setBackground('image', 'https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg').setColors({ box: '#212121', username: '#ffffff', score: '#ffffff', firstRank: '#f7c716', secondRank: '#9e9e9e', thirdRank: '#94610f' }).setUsersData(canvafyUsers).build();
      const attachment = new AttachmentBuilder(imageBuffer, { name: `message-leaderboard-${gId}.png` });

      let msgContent = `🎉 **Message Leaders for the week in ${currentGuild.name}!** 🔥\n\n`;
      topUsersData.slice(0, 3).forEach((user, index) => {
        const medal = ['🏆', '🥈', '🥉'][index];
        msgContent += `${medal} **${index+1}${index === 0 ? 'st' : index === 1 ? 'nd' : 'rd'} Place:** <@${user.userId}> (${user.username}) - ${user.score} messages\n`;
      });
      msgContent += `\nKeep up the great engagement!`;

      const announcementChannel = currentGuild.channels.cache.get(config.messageLeaderSystem.channelMessageLeader);
      if (!announcementChannel?.isTextBased()) {
        console.error(`[Announce Leaders] Announcement channel ${config.messageLeaderSystem.channelMessageLeader} not found or not text-based in guild ${currentGuild.name} (${gId}). Skipping send.`);
        continue;
      }

      await announcementChannel.send({ content: msgContent, files: [attachment] }).catch(e => console.error(`[Announce Leaders] Failed to send announcement to channel ${announcementChannel.id} in guild ${gId}: ${e.message}`));

      const leaderRoleId = config.messageLeaderSystem.roleMessageLeader;
      if (leaderRoleId) {
        const leaderRole = currentGuild.roles.cache.get(leaderRoleId);
        if (leaderRole) {
          console.log(`[Announce Leaders] Managing winner role ${leaderRole.name} (${leaderRoleId}) in guild ${gId}.`);
          for (const member of leaderRole.members.values()) {
            if (!topUsersData.slice(0, 1).some(w => w.userId === member.id)) {
              await member.roles.remove(leaderRole, 'End of weekly message leader term').catch(e => console.error(`[Announce Leaders] Failed removing role ${leaderRole.id} from ${member.id} in guild ${gId}: ${e.message}`));
            }
          }
          const winnerMember = currentGuild.members.cache.get(topUsersData[0].userId);
          if (winnerMember && !winnerMember.roles.cache.has(leaderRole.id)) {
            await winnerMember.roles.add(leaderRole, 'Weekly Message Leader').catch(e => console.error(`[Announce Leaders] Failed adding role ${leaderRole.id} to winner ${winnerMember.id} in guild ${gId}: ${e.message}`));
          }
        } else {
          console.warn(`[Announce Leaders] Configured winner role ${leaderRoleId} not found in guild ${gId}.`);
        }
      }

      await incrementMessageLeaderWins(gId, topUsersData[0].userId).catch(e => console.error(`[Announce Leaders] Failed to increment message leader wins for ${topUsersData[0].userId} in guild ${gId}: ${e.message}`));

      console.log(`[Announce Leaders] Starting weekly message count reset for guild ${gId}.`);
      let usersToReset;
      try {
        usersToReset = await listUserData(gId);
      } catch (listError) {
        console.error(`[Announce Leaders] Failed to list users for message reset in guild ${gId}:`, listError);
        usersToReset = [];
      }

      const resetPromises = usersToReset.map(({ userId }) =>
          updateUserData(gId, userId, { messages: 0 }).catch(err => {
            console.error(`[Announce Leaders] Failed message reset for user ${userId} in guild ${gId}:`, err);

          })
      );
      await Promise.all(resetPromises);
      console.log(`[Announce Leaders] Finished weekly message count reset for guild ${gId}. Reset attempted for ${usersToReset.length} users.`);

      processedGuilds++;
    } catch (error) {
      console.error(`[Announce Leaders] Unhandled error processing guild ${currentGuild.name} (${gId}):`, error);
      failedGuilds++;
    }
  }
  console.log(`[Announce Leaders] Finished process. Guilds Processed: ${processedGuilds}, Guilds Failed: ${failedGuilds}.`);
}


async function assignRole(guildId, userId, roleId, reason = 'Automated role assignment') {
  const guild = client.guilds.cache.get(guildId);
  if (!guild) { console.warn(`[assignRole] Guild ${guildId} not found.`); return false; }
  const role = guild.roles.cache.get(roleId);
  if (!role) { console.warn(`[assignRole] Role ${roleId} not found in guild ${guild.name}.`); return false; }
  try {
    const member = await guild.members.fetch(userId).catch(()=>null);
    if (!member) {
      console.warn(`[assignRole] Member ${userId} not found in guild ${guild.name} (${guildId}).`);
      return false;
    }
    if (guild.members.me && role.position >= guild.members.me.roles.highest.position) {
      console.warn(`[assignRole] Cannot assign role ${role.name} (${roleId}) - Bot role hierarchy issue in ${guild.name}.`);
      return false;
    }
    if (!member.roles.cache.has(role.id)) {
      await member.roles.add(role, reason);
      console.log(`[assignRole] Successfully assigned role ${role.name} (${roleId}) to user ${userId} in guild ${guild.name}. Reason: ${reason}`);
      return true;
    } else {
      console.log(`[assignRole] User ${userId} already has role ${role.name} (${roleId}) in guild ${guild.name}. No action needed.`);
      return true;
    }
  } catch (error) {
    if (error.code === 50013) {
      console.error(`[assignRole] Missing Permissions to assign role ${role.name} (${roleId}) in guild ${guild.name}.`);
    } else if (error.code === 10007) {
      console.warn(`[assignRole] Member ${userId} likely left guild ${guild.name} during role assignment.`);
    } else if (error.code === 10011) {
      console.warn(`[assignRole] Role ${roleId} likely deleted during assignment in guild ${guild.name}.`);
    }
    else {
      console.error(`[assignRole] Error assigning role ${roleId} to ${userId} in guild ${guild.name}:`, error);
    }
    return false;
  }
}

async function removeRole(guildId, userId, roleId, reason = 'Automated role removal') {
  const guild = client.guilds.cache.get(guildId);
  if (!guild) { console.warn(`[removeRole] Guild ${guildId} not found.`); return false; }
  const role = guild.roles.cache.get(roleId);
  if (!role) { console.warn(`[removeRole] Role ${roleId} not found in guild ${guild.name}.`); return false; }
  try {
    const member = await guild.members.fetch(userId).catch(()=>null);
    if (!member) {
      console.warn(`[removeRole] Member ${userId} not found in guild ${guild.name} (${guildId}).`);
      return false;
    }
    if (guild.members.me && role.position >= guild.members.me.roles.highest.position) {
      console.warn(`[removeRole] Cannot remove role ${role.name} (${roleId}) - Bot role hierarchy issue in ${guild.name}.`);
      return false;
    }
    if (member.roles.cache.has(role.id)) {
      await member.roles.remove(role, reason);
      console.log(`[removeRole] Successfully removed role ${role.name} (${roleId}) from user ${userId} in guild ${guild.name}. Reason: ${reason}`);
      return true;
    } else {
      console.log(`[removeRole] User ${userId} does not have role ${role.name} (${roleId}) in guild ${guild.name}. No action needed.`);
      return true;
    }
  } catch (error) {
    if (error.code === 50013) {
      console.error(`[removeRole] Missing Permissions to remove role ${role.name} (${roleId}) in guild ${guild.name}.`);
    } else if (error.code === 10007) {
      console.warn(`[removeRole] Member ${userId} likely left guild ${guild.name} during role removal.`);
    } else if (error.code === 10011) {
      console.warn(`[removeRole] Role ${roleId} likely deleted during removal in guild ${guild.name}.`);
    } else {
      console.error(`[removeRole] Error removing role ${roleId} from ${userId} in guild ${guild.name}:`, error);
    }
    return false;
  }
}

async function resetDailyStreaks() {
  console.log("[Daily Reset] Starting daily reset process...");
  const guilds = client.guilds.cache.map(g => g.id);
  const today = new Date().toISOString().split('T')[0];
  let totalUsersProcessed = 0;
  let totalUsersUpdated = 0;
  let totalFailures = 0;

  for (const gId of guilds) {
    const currentGuild = client.guilds.cache.get(gId);
    if (!currentGuild) {
      console.warn(`[Daily Reset] Guild ${gId} not found in cache. Skipping.`);
      continue;
    }

    let config;
    try {
      config = await getConfig(gId);
      if (!config) {
        console.warn(`[Daily Reset] Config not found for guild ${currentGuild.name} (${gId}). Skipping.`);
        continue;
      }
    } catch (configError) {
      console.error(`[Daily Reset] Failed to get config for guild ${currentGuild.name} (${gId}):`, configError);
      continue;
    }

    console.log(`[Daily Reset] Processing guild: ${currentGuild.name} (${gId})`);
    const streakEnabled = config.streakSystem?.enabled ?? false;
    const baseThreshold = safeParseNumber(config.streakSystem?.streakThreshold, 10);

    let usersData;
    try {
      usersData = await listUserData(gId);
      console.log(`[Daily Reset] Found ${usersData.length} user data records in guild ${gId}.`);
    } catch (listError) {
      console.error(`[Daily Reset] CRITICAL: Failed to list user data for guild ${gId}:`, listError);
      continue;
    }

    const updatePromises = [];
    let guildUsersProcessed = 0;
    let guildFailures = 0;

    for (const { userId, userData } of usersData) {
      guildUsersProcessed++;
      totalUsersProcessed++;
      const userUpdates = {};
      let userLostStreak = false;
      const oldStreak = safeParseNumber(userData.streak, 0);

      if (userData.receivedDaily === true) {
        userUpdates.receivedDaily = false;
      }

      if (streakEnabled && safeParseNumber(userData.threshold, baseThreshold) !== baseThreshold) {
        userUpdates.threshold = baseThreshold;
      }

      if (streakEnabled && oldStreak > 0 && userData.receivedDaily !== true) {
        console.log(`[Daily Reset] User ${userId} in guild ${gId} did not meet threshold (receivedDaily: ${userData.receivedDaily}). Streak reset from ${oldStreak} to 0.`);
        userUpdates.streak = 0;
        userUpdates.lastStreakLoss = new Date().toISOString();
        userLostStreak = true;
      } else if (streakEnabled && oldStreak === 0 && userData.receivedDaily !== true) {

      }

      const trackedDays = safeParseNumber(userData.daysTracked, 0) + 1;
      userUpdates.daysTracked = trackedDays;


      const isActiveToday = Array.isArray(userData.messageHeatmap) && userData.messageHeatmap.some(entry => entry.date === today && safeParseNumber(entry.messages, 0) > 0);
      const consecutiveInactive = isActiveToday ? 0 : (safeParseNumber(userData.consecutiveInactiveDays, 0) + 1);


      if (isActiveToday) {
        userUpdates.activeDaysCount = safeParseNumber(userData.activeDaysCount, 0) + 1;
      }
      if (consecutiveInactive > safeParseNumber(userData.longestInactivePeriod, 0)) {
        userUpdates.longestInactivePeriod = consecutiveInactive;
      }
      if (trackedDays > 0) {
        const totalMessages = safeParseNumber(userData.totalMessages, 0);
        const avg = totalMessages / trackedDays;
        userUpdates.averageMessagesPerDay = parseFloat(avg.toFixed(4));
      } else {
        userUpdates.averageMessagesPerDay = 0;
      }


      if (Object.keys(userUpdates).length > 0) {
        updatePromises.push(
            updateUserData(gId, userId, userUpdates)
                .then(async () => {
                  totalUsersUpdated++;
                  if (userLostStreak && streakEnabled) {
                    await removeStreakRoles(gId, userId, config, oldStreak);
                  }
                })
                .catch(err => {
                  console.error(`[Daily Reset] Failed daily update for user ${userId} in guild ${gId}:`, err);
                  guildFailures++;
                  totalFailures++;
                })
        );
      }
    }

    try {
      await Promise.all(updatePromises);
      console.log(`[Daily Reset] Finished processing guild ${currentGuild.name} (${gId}). Users processed: ${guildUsersProcessed}, Updates attempted: ${updatePromises.length}, Failures: ${guildFailures}.`);
    } catch (batchError) {

      console.error(`[Daily Reset] Error during Promise.all for guild ${gId}:`, batchError);
    }
  }
  console.log(`[Daily Reset] Daily reset cycle finished. Total Users Processed: ${totalUsersProcessed}, Total Updates Successful: ${totalUsersUpdated}, Total Failures: ${totalFailures}.`);
}


async function removeStreakRoles(guildId, userId, config, oldStreakValue) {
  if (!config?.streakSystem?.enabled || oldStreakValue <= 0) return;
  const guild = client.guilds.cache.get(guildId);
  if (!guild) { console.warn(`[Remove Roles] Guild ${guildId} not found.`); return; }
  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) { console.warn(`[Remove Roles] Member ${userId} not found in guild ${guild.name}.`); return; }

  const rolesToRemove = [];
  for (const key in config.streakSystem) {
    if (key.startsWith('role') && key.endsWith('day')) {
      const days = parseInt(key.replace('role', '').replace('day', ''), 10);
      if (!isNaN(days) && oldStreakValue >= days) {
        const roleId = config.streakSystem[key];
        if (!roleId) continue;
        const role = guild.roles.cache.get(roleId);
        if (role && member.roles.cache.has(roleId)) {
          if (guild.members.me && role.position < guild.members.me.roles.highest.position) {
            rolesToRemove.push(roleId);
          } else {
            console.warn(`[Remove Roles] Cannot remove role ${role.name} (${roleId}) from ${userId} in ${guild.name} - hierarchy issue or bot missing role.`);
          }
        }
      }
    }
  }

  if (rolesToRemove.length > 0) {
    console.log(`[Remove Roles] Attempting to remove roles [${rolesToRemove.join(', ')}] from user ${userId} in guild ${guild.name} due to lost ${oldStreakValue}-day streak.`);
    try {
      await member.roles.remove(rolesToRemove, `Lost ${oldStreakValue}-day streak`);
      console.log(`[Remove Roles] Successfully removed roles from ${userId}.`);
      await member.send(`You lost your **${oldStreakValue}-day** message streak in **${guild.name}**...`).catch(dmError => {
        if (dmError.code !== 50007) {
          console.warn(`[Remove Roles] Failed to send DM to user ${userId} (${guild.name}): ${dmError.message}`);
        }
      });
    } catch (error) {
      if (error.code === 50013) {
        console.error(`[Remove Roles] Missing Permissions to remove roles [${rolesToRemove.join(', ')}] from ${userId} in ${guild.name}.`);
      } else {
        console.error(`[Remove Roles] Error removing roles from ${userId} in ${guild.name}:`, error);
      }
    }
  } else {
    console.log(`[Remove Roles] No roles found to remove for user ${userId} in guild ${guild.name} after losing ${oldStreakValue}-day streak.`);
  }
}

async function generateWeeklyReport(guildId) {
  const currentGuild = client.guilds.cache.get(guildId);
  if (!currentGuild) return;
  const config = await getConfig(guildId);
  if (!config?.reportSettings?.weeklyReportChannel) return;
  const reportChannel = currentGuild.channels.cache.get(config.reportSettings.weeklyReportChannel);
  if (!reportChannel?.isTextBased()) {
    console.warn(`[Weekly Report] Channel ${config.reportSettings.weeklyReportChannel} not found or not text-based in guild ${guildId}.`);
    return;
  }
  try {
    const usersData = await listUserData(guildId);
    if (!usersData || usersData.length === 0) {
      await reportChannel.send(`**Weekly Report - ${currentGuild.name}**: No user data found for this period.`);
      return;
    }
    let totalMsg = 0, activeUsers = 0, totalStr = 0, usersStr = 0, highestStr = 0, highestLvl = 0;
    usersData.forEach(({ userData: ud }) => {
      const weeklyMsgs = safeParseNumber(ud.messages, 0);
      totalMsg += weeklyMsgs;
      if (weeklyMsgs > 0) activeUsers++;
      const currentStreak = safeParseNumber(ud.streak, 0);
      if (currentStreak > 0) {
        totalStr += currentStreak;
        usersStr++;
      }
      highestStr = Math.max(highestStr, safeParseNumber(ud.highestStreak, 0));
      highestLvl = Math.max(highestLvl, safeParseNumber(ud.experience?.level, 0));
    });
    const avgMsg = activeUsers > 0 ? (totalMsg / activeUsers).toFixed(2) : '0.00';
    const avgStr = usersStr > 0 ? (totalStr / usersStr).toFixed(2) : '0.00';
    const embed = new EmbedBuilder().setColor('#3498DB').setTitle(`Weekly Activity Report - ${currentGuild.name}`)
        .setDescription(`Summary for the week ending ${new Date().toLocaleDateString()}`)
        .addFields(
            {name: 'Messages Sent (Week)', value: `${totalMsg}`, inline: true },
            { name: 'Active Users (Week)', value: `${activeUsers}`, inline: true },
            { name: 'Avg Msgs/Active User', value: `${avgMsg}`, inline: true },
            { name: 'Current Highest Streak', value: `${highestStr}`, inline: true },
            { name: 'Users w/ Active Streaks', value: `${usersStr}`, inline: true },
            { name: 'Avg Active Streak', value: `${avgStr}`, inline: true },
            { name: 'Highest Level Reached', value: `${highestLvl}`, inline: true }
        ).setTimestamp().setFooter({ text: `Guild ID: ${guildId}` });
    const topMsgrs = await queryLeaderboard('messages', guildId, 3);
    if (topMsgrs.length > 0) {
      const topMsgrsText = topMsgrs.map((item, idx) => {
        const userMention = `<@${item.discordAccountId}>`;
        const count = safeParseNumber(item.count, 0);
        return `${idx+1}. ${userMention} (${count})`;
      }).join('\n');
      embed.addFields({ name: 'Top Messagers (Week)', value: topMsgrsText || 'N/A', inline: false });
    }
    await reportChannel.send({ embeds: [embed] });
  } catch (err) {
    console.error(`[Weekly Report] Error generating report for guild ${guildId}:`, err);
    try {
      await reportChannel.send(`An error occurred while generating the weekly report for ${currentGuild.name}.`);
    } catch (sendError) {
      console.error(`[Weekly Report] Failed to send error message to report channel ${reportChannel.id} in guild ${guildId}:`, sendError);
    }
  }
}

async function generateMonthlyReport(guildId) {
  const currentGuild = client.guilds.cache.get(guildId);
  if (!currentGuild) return;
  const config = await getConfig(guildId);
  if (!config?.reportSettings?.monthlyReportChannel) return;
  const reportChannel = currentGuild.channels.cache.get(config.reportSettings.monthlyReportChannel);
  if (!reportChannel?.isTextBased()) {
    console.warn(`[Monthly Report] Channel ${config.reportSettings.monthlyReportChannel} not found or not text-based in guild ${guildId}.`);
    return;
  }
  try {
    const usersData = await listUserData(guildId);
    if (!usersData || usersData.length === 0) {
      await reportChannel.send(`**Monthly Report - ${currentGuild.name}**: No user data found.`);
      return;
    }
    let totalLifeMsg = 0, totalUsers = usersData.length, highestStrAllTime = 0, highestLvlAllTime = 0, totalWins = 0;
    usersData.forEach(({ userData: ud }) => {
      totalLifeMsg += safeParseNumber(ud.totalMessages, 0);
      highestStrAllTime = Math.max(highestStrAllTime, safeParseNumber(ud.highestStreak, 0));
      highestLvlAllTime = Math.max(highestLvlAllTime, safeParseNumber(ud.experience?.level, 0));
      totalWins += safeParseNumber(ud.messageLeaderWins, 0);
    });
    const avgLifeMsg = totalUsers > 0 ? (totalLifeMsg / totalUsers).toFixed(2) : '0.00';
    const embed = new EmbedBuilder().setColor('#9B59B6').setTitle(`Monthly Activity Report - ${currentGuild.name}`)
        .setDescription(`Summary for ${new Date().toLocaleDateString('default', { month: 'long', year: 'numeric' })}`)
        .addFields(
            { name: 'Total Lifetime Msgs', value: `${totalLifeMsg}`, inline: true },
            { name: 'Total Tracked Users', value: `${totalUsers}`, inline: true },
            { name: 'Avg Lifetime Msgs/User', value: `${avgLifeMsg}`, inline: true },
            { name: 'All-Time Highest Streak', value: `${highestStrAllTime}`, inline: true },
            { name: 'All-Time Highest Level', value: `${highestLvlAllTime}`, inline: true },
            { name: 'Total Leader Wins (All Time)', value: `${totalWins}`, inline: true }
        ).setTimestamp().setFooter({ text: `Guild ID: ${guildId}` });
    const topStreakers = await queryLeaderboard('highestStreak', guildId, 3);
    if (topStreakers.length > 0) {
      const topStreakersText = topStreakers.map((item, idx) => {
        const userMention = `<@${item.discordAccountId}>`;
        const count = safeParseNumber(item.count, 0);
        return `${idx+1}. ${userMention} (${count})`;
      }).join('\n');
      embed.addFields({ name: 'Top Highest Streaks (All Time)', value: topStreakersText || 'N/A', inline: false });
    }
    await reportChannel.send({ embeds: [embed] });
  } catch (err) {
    console.error(`[Monthly Report] Error generating report for guild ${guildId}:`, err);
    try {
      await reportChannel.send(`An error occurred while generating the monthly report for ${currentGuild.name}.`);
    } catch (sendError) {
      console.error(`[Monthly Report] Failed to send error message to report channel ${reportChannel.id} in guild ${guildId}:`, sendError);
    }
  }
}

async function sendConfigMessage(guild) {
  try {
    let channelToSend = guild.systemChannel ||
        guild.publicUpdatesChannel ||
        guild.channels.cache.find(ch => ch.type === 0 && ch.permissionsFor(guild.members.me).has('SendMessages'));
    if (channelToSend) {
      let adder = null;
      try {
        if (guild.members.me?.permissions.has('ViewAuditLog')) {
          const logs = await guild.fetchAuditLogs({ type: 28, limit: 1 });
          const entry = logs.entries.first();
          if (entry && entry.target?.id === client.user.id && (Date.now() - entry.createdTimestamp < 300000)) {
            adder = entry.executor;
          }
        }
      } catch (auditLogError) {
        console.warn(`[Config Msg] Failed to fetch audit log in guild ${guild.id}: ${auditLogError.message}`);
      }
      let msgContent = `Hello! Thanks for adding me to **${guild.name}**!`;
      if (adder) {
        msgContent = `Hello ${adder}! Thanks for adding me to **${guild.name}**!`;
      }
      msgContent += `\n\nUse \`/setup-bot\` or \`/stitches-configuration\` to configure features like streaks, levels, and reports.`;
      await channelToSend.send(msgContent);
    } else {
      console.warn(`[Config Msg] Could not find a suitable channel to send the welcome message in guild ${guild.id} (${guild.name}).`);
    }
  } catch (err) {
    console.error(`[Config Msg] Error sending config message in guild ${guild.id} (${guild.name}): ${err}`);
  }
}


async function handleUserMessage(guildId, userId, channel, message) {
  const now = Date.now();
  const cooldownTime = 1000;
  const lastProcessed = streakCooldowns.get(userId) || 0;
  if (now - lastProcessed < cooldownTime) {
    return;
  }
  streakCooldowns.set(userId, now);

  try {
    const config = await getConfig(guildId);
    if (!config) {
      console.warn(`[Handle Msg] No config found for guild ${guildId}. Skipping message processing for user ${userId}.`);
      return;
    }
    const currentGuild = client.guilds.cache.get(guildId);
    if (!currentGuild) {
      console.warn(`[Handle Msg] Guild ${guildId} not found in cache. Skipping message processing for user ${userId}.`);
      return;
    }

    const userData = await getUserData(guildId, userId);
    let userRec = null;
    let needsImmediateSave = false;

    if (!userData) {
      console.log(`[Handle Msg] New user detected: ${userId} in guild ${guildId}. Initializing data.`);
      userRec = {
        streak: 0, highestStreak: 0, messages: 0,
        threshold: safeParseNumber(config.streakSystem?.streakThreshold, 10),
        receivedDaily: false, messageLeaderWins: 0, highestMessageCount: 0,
        mostConsecutiveLeader: 0, totalMessages: 0, daysTracked: 0,
        averageMessagesPerDay: 0.0, activeDaysCount: 0, longestInactivePeriod: 0,
        lastStreakLoss: null, messageHeatmap: [], milestones: [], rolesAchieved: [],
        experience: { totalXp: 0, level: 0 }, boosters: 1.0,
        lastMessage: { time: 0, content: '', date: null },
        channelsParticipated: [], mentionsRepliesCount: { mentions: 0, replies: 0 }
      };
      needsImmediateSave = true;
    } else if (userData.userData && typeof userData.userData === 'object') {
      userRec = userData.userData;


      if (!userRec.experience || typeof userRec.experience !== 'object') userRec.experience = { totalXp: 0, level: 0 };
      if (!userRec.lastMessage || typeof userRec.lastMessage !== 'object') userRec.lastMessage = { time: 0, content: '', date: null };
      if (!userRec.mentionsRepliesCount || typeof userRec.mentionsRepliesCount !== 'object') userRec.mentionsRepliesCount = { mentions: 0, replies: 0 };
      if (typeof userRec.boosters !== 'number') userRec.boosters = safeParseNumber(userRec.boosters, 1.0);
      if (typeof userRec.messages !== 'number') userRec.messages = safeParseNumber(userRec.messages, 0);
      if (typeof userRec.totalMessages !== 'number') userRec.totalMessages = safeParseNumber(userRec.totalMessages, 0);
      if (typeof userRec.streak !== 'number') userRec.streak = safeParseNumber(userRec.streak, 0);
      if (typeof userRec.highestStreak !== 'number') userRec.highestStreak = safeParseNumber(userRec.highestStreak, 0);
      if (typeof userRec.experience.level !== 'number') userRec.experience.level = safeParseNumber(userRec.experience.level, 0);
      if (typeof userRec.experience.totalXp !== 'number') userRec.experience.totalXp = safeParseNumber(userRec.experience.totalXp, 0);


    } else {
      console.log(`[Handle Msg] Old data format detected for user ${userId} in guild ${guildId}. Migrating...`);
      const oldData = userData;
      const baseThreshold = safeParseNumber(config.streakSystem?.streakThreshold, 10);

      userRec = {
        streak: safeParseNumber(oldData.streak, 0),
        highestStreak: safeParseNumber(oldData.highestStreak, safeParseNumber(oldData.streak, 0)),
        messages: safeParseNumber(oldData.messages, 0),
        threshold: safeParseNumber(oldData.threshold, baseThreshold),
        receivedDaily: oldData.receivedDaily === true,
        messageLeaderWins: safeParseNumber(oldData.messageLeaderWins, 0),
        highestMessageCount: safeParseNumber(oldData.highestMessageCount, 0),
        mostConsecutiveLeader: safeParseNumber(oldData.mostConsecutiveLeader, 0),
        totalMessages: safeParseNumber(oldData.totalMessages, safeParseNumber(oldData.messages, 0)),
        daysTracked: safeParseNumber(oldData.daysTracked, 0),
        averageMessagesPerDay: safeParseNumber(oldData.averageMessagesPerDay, 0.0),
        activeDaysCount: safeParseNumber(oldData.activeDaysCount, 0),
        longestInactivePeriod: safeParseNumber(oldData.longestInactivePeriod, 0),
        lastStreakLoss: oldData.lastStreakLoss || null,
        messageHeatmap: Array.isArray(oldData.messageHeatmap) ? oldData.messageHeatmap : [],
        milestones: Array.isArray(oldData.milestones) ? oldData.milestones : [],
        rolesAchieved: Array.isArray(oldData.rolesAchieved) ? oldData.rolesAchieved : [],
        experience: {
          totalXp: safeParseNumber(oldData.experience?.totalXp, safeParseNumber(oldData.totalXp, safeParseNumber(oldData.experience, 0))),
          level: safeParseNumber(oldData.experience?.level, safeParseNumber(oldData.level, 0))
        },
        boosters: safeParseNumber(oldData.boosters, 1.0),
        lastMessage: {
          time: safeParseNumber(oldData.lastMessage?.time, safeParseNumber(oldData.lastMessageTime, 0)),
          content: oldData.lastMessage?.content || '',
          date: oldData.lastMessage?.date || null
        },
        channelsParticipated: Array.isArray(oldData.channelsParticipated) ? oldData.channelsParticipated : [],
        mentionsRepliesCount: {
          mentions: safeParseNumber(oldData.mentionsRepliesCount?.mentions, safeParseNumber(oldData.mentionsCount, 0)),
          replies: safeParseNumber(oldData.mentionsRepliesCount?.replies, safeParseNumber(oldData.repliesCount, 0))
        }
      };
      console.log(`[Handle Msg] Finished mapping old data for user ${userId}.`);
      needsImmediateSave = true;
    }

    if (needsImmediateSave) {
      try {
        console.log(`[Handle Msg] Performing initial/migration save for user ${userId} in guild ${guildId}.`);
        await saveUserData(guildId, userId, userRec);
        console.log(`[Handle Msg] Initial/migration save successful for user ${userId}.`);

        const freshlySavedData = await getUserData(guildId, userId);
        if (freshlySavedData) {
          userRec = freshlySavedData;
        } else {
          console.error(`[Handle Msg] CRITICAL: Failed to retrieve data immediately after saving for user ${userId}. Aborting further processing for this message.`);
          return;
        }
      } catch (saveError) {
        console.error(`[Handle Msg] CRITICAL: Failed initial/migration save for user ${userId} in guild ${guildId}:`, saveError);
        return;
      }
    }


    const updates = {};
    const todayISO = new Date().toISOString().split('T')[0];


    updates.lastMessage = { time: now, content: message.content.substring(0, 200), date: todayISO };

    userRec.totalMessages = safeParseNumber(userRec.totalMessages, 0) + 1;
    updates.totalMessages = userRec.totalMessages;

    userRec.messages = safeParseNumber(userRec.messages, 0) + 1;
    updates.messages = userRec.messages;


    if (!Array.isArray(userRec.channelsParticipated)) {
      userRec.channelsParticipated = [];
    }
    if (!userRec.channelsParticipated.includes(channel.id)) {
      const currentChannels = [...userRec.channelsParticipated];
      currentChannels.push(channel.id);
      userRec.channelsParticipated = currentChannels.slice(-20);
      updates.channelsParticipated = userRec.channelsParticipated;
    }


    const currentHeatmap = Array.isArray(userRec.messageHeatmap) ? [...userRec.messageHeatmap] : [];
    let todayEntry = currentHeatmap.find(entry => entry.date === todayISO);
    if (todayEntry) {
      todayEntry.messages = safeParseNumber(todayEntry.messages, 0) + 1;
    } else {
      currentHeatmap.push({ date: todayISO, messages: 1 });
    }

    if (currentHeatmap.length > 60) {
      currentHeatmap.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());
      userRec.messageHeatmap = currentHeatmap.slice(0, 60);
    } else {
      userRec.messageHeatmap = currentHeatmap;
    }
    updates.messageHeatmap = userRec.messageHeatmap;


    let streakIncreasedToday = false;
    let milestoneAchieved = null;
    let milestoneRoleId = null;
    let currentStreakValue = safeParseNumber(userRec.streak, 0);

    if (config.streakSystem?.enabled && userRec.receivedDaily !== true) {
      let currentThreshold = safeParseNumber(userRec.threshold, safeParseNumber(config.streakSystem.streakThreshold, 10));

      if (currentThreshold > 0) {
        currentThreshold -= 1;
        userRec.threshold = currentThreshold;
        updates.threshold = currentThreshold;
      }

      if (currentThreshold === 0) {
        currentStreakValue = safeParseNumber(userRec.streak, 0) + 1;
        userRec.streak = currentStreakValue;
        updates.streak = currentStreakValue;

        userRec.receivedDaily = true;
        updates.receivedDaily = true;
        streakIncreasedToday = true;

        let currentHighestStreak = safeParseNumber(userRec.highestStreak, 0);
        if (currentStreakValue > currentHighestStreak) {
          userRec.highestStreak = currentStreakValue;
          updates.highestStreak = currentStreakValue;
        }


        for (const key in config.streakSystem) {
          if (key.startsWith('role') && key.endsWith('day')) {
            const days = parseInt(key.replace('role','').replace('day',''), 10);
            if (!isNaN(days) && currentStreakValue >= days) {
              const roleId = config.streakSystem[key];
              if (roleId && !userRec.rolesAchieved.includes(roleId)) {
                userRec.rolesAchieved.push(roleId);
                updates.rolesAchieved = userRec.rolesAchieved;
                userRec.milestones.push({ milestone: days, date: new Date().toISOString() });
                updates.milestones = userRec.milestones;
                await assignRole(guildId, userId, roleId, `${days}-Day Streak Achieved`);
              }
            }
          }
        }
      }
    }


    let levelIncreasedToday = false;
    let currentLevelValue = safeParseNumber(userRec.experience?.level, 0);
    let levelUpRoleId = null;

    if (config.levelSystem?.enabled) {
      const xpGainBase = safeParseNumber(config.levelSystem.xpPerMessage, 10);
      const booster = safeParseNumber(userRec.boosters, 1.0);
      const xpGain = Math.floor(xpGainBase * booster);

      if (xpGain > 0) {
        let currentXp = safeParseNumber(userRec.experience?.totalXp, 0);
        let previousLevel = currentLevelValue;
        currentXp += xpGain;

        const baseXP = safeParseNumber(config.levelSystem.baseXp, 100); // Added baseXP config option
        const multiplier = safeParseNumber(config.levelSystem.levelMultiplier, 1.5);
        let xpNeededForNextLevel = Math.floor(baseXP * Math.pow(multiplier, currentLevelValue));


        while (currentXp >= xpNeededForNextLevel && currentLevelValue < 100) {
          currentXp -= xpNeededForNextLevel;
          currentLevelValue++;
          levelIncreasedToday = true;
          xpNeededForNextLevel = Math.floor(baseXP * Math.pow(multiplier, currentLevelValue));
        }

        userRec.experience.totalXp = currentXp;
        updates['experience.totalXp'] = currentXp;

        if (levelIncreasedToday) {
          userRec.experience.level = currentLevelValue;
          updates['experience.level'] = currentLevelValue;
          console.log(`[Handle Msg] User ${userId} leveled up from ${previousLevel} to ${currentLevelValue} in guild ${guildId}.`);

          const roleKey = `roleLevel${currentLevelValue}`;
          const roleId = config.levelSystem[roleKey];
          if (roleId) {
            levelUpRoleId = roleId;
            const currentRolesAchieved = Array.isArray(userRec.rolesAchieved) ? [...userRec.rolesAchieved] : [];
            if (!currentRolesAchieved.includes(roleId)) {
              currentRolesAchieved.push(roleId);
              userRec.rolesAchieved = currentRolesAchieved;
              updates.rolesAchieved = currentRolesAchieved;
            }
            await assignRole(guildId, userId, roleId, `Reached Level ${currentLevelValue}`);
          }
        }
      }
    }


    if (Object.keys(updates).length > 0) {
      try {
        await updateUserData(guildId, userId, updates);
      } catch (updateError) {
        console.error(`[Handle Msg] Failed to apply updates for user ${userId} in guild ${guildId}:`, updateError);

        return;
      }
    } else {
      console.log(`[Handle Msg] No updates detected for user ${userId} in guild ${guildId} after processing message.`);
    }


    if (streakIncreasedToday && config.streakSystem?.enabled) {
      const streakChannelId = config.streakSystem.channelStreakOutput;
      let streakAnnounceChannel = streakChannelId ? currentGuild.channels.cache.get(streakChannelId) : null;
      if (!streakAnnounceChannel?.isTextBased()) {
        streakAnnounceChannel = channel;
      }
      if (streakAnnounceChannel?.isTextBased()) {
        try {
          const userForAvatar = await client.users.fetch(userId);
          const avatarUrl = userForAvatar.displayAvatarURL({ format: 'png', size: 128 });
          const previousStreak = safeParseNumber(userRec.streak, 1) - 1;

          const streakImage = await new canvafy.LevelUp()
              .setAvatar(avatarUrl)
              .setBackground("image","https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg")
              .setUsername(userForAvatar.username)
              .setBorder("#FF0000")
              .setAvatarBorder("#FFFFFF")
              .setOverlayOpacity(0.7)
              .setLevels(previousStreak, userRec.streak)
              .build();
          const attachment = new AttachmentBuilder(streakImage, { name:`streak-${userId}-${userRec.streak}.png` });

          let streakMessage = `🎉 <@${userId}> has increased their message streak to **${userRec.streak}** days!`;
          if (milestoneAchieved && milestoneRoleId) {
            const role = currentGuild.roles.cache.get(milestoneRoleId);
            streakMessage += `\nThey've earned the **${role ? role.name : `Milestone Role`}**!`;
          }
          await streakAnnounceChannel.send({ content: streakMessage, files: [attachment] });
        } catch (announceError) {
          console.error(`[Handle Msg] Failed streak announcement for user ${userId} in guild ${guildId}:`, announceError);
        }
      } else {
        console.warn(`[Handle Msg] Could not find a suitable text channel for streak announcement for user ${userId} in guild ${guildId}. Target channel ID: ${streakChannelId}`);
      }
    }

    if (levelIncreasedToday && config.levelSystem?.enabled && config.levelSystem.levelUpMessages !== false) {
      const levelChannelId = config.levelSystem.channelLevelUp;
      let levelAnnounceChannel = levelChannelId ? currentGuild.channels.cache.get(levelChannelId) : null;
      if (!levelAnnounceChannel?.isTextBased()) {
        levelAnnounceChannel = channel;
      }
      if (levelAnnounceChannel?.isTextBased()) {
        try {
          let levelMessage = `🎉 Congrats <@${userId}>! You reached **Level ${userRec.experience.level}**!`;
          if (levelUpRoleId) {
            const role = currentGuild.roles.cache.get(levelUpRoleId);
            levelMessage += ` You earned the **${role ? role.name : `Level Role`}**!`;
          }
          await levelAnnounceChannel.send(levelMessage);
        } catch (announceError) {
          console.error(`[Handle Msg] Failed level up announcement for user ${userId} in guild ${guildId}:`, announceError);
        }
      } else {
        console.warn(`[Handle Msg] Could not find a suitable text channel for level up announcement for user ${userId} in guild ${guildId}. Target channel ID: ${levelChannelId}`);
      }
    }

  } catch (error) {
    console.error(`[Handle Msg] Unhandled error in handleUserMessage for user ${userId} guild ${guildId}:`, error);
  }
}


client.on('guildMemberRemove', async (member) => {
  console.log(`User ${member.user.tag} (${member.id}) left guild ${member.guild.name} (${member.guild.id})`);

});

client.login(token).catch(err => {
  console.error("Failed to login to Discord:", err);
  process.exit(1);
});
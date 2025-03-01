require('dotenv').config();
const { Client, GatewayIntentBits } = require('discord.js');
const { getConfig, listUserData, updateUserData } = require('./dynamoDB');

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMembers,
    ],
});

const token = process.env.TOKEN;

async function resetDailyForAllUsers() {
    try {
        console.log("Starting manual daily reset...");

        const guilds = client.guilds.cache.map(guild => guild.id);

        for (const guildId of guilds) {
            const config = await getConfig(guildId);
            if (!config || !config.streakSystem || !config.streakSystem.enabled) continue;

            const messageThreshold = config.streakSystem.streakThreshold || 10;
            const today = new Date().toISOString().split('T')[0];

            const users = await listUserData();
            for (const { userId, userData } of users) {
                if (!userData) continue;

                const updateFields = {
                    receivedDaily: false,
                    threshold: messageThreshold,
                };

                if (!userData.messageHeatmap.some(entry => entry.date === today)) {
                    userData.messageHeatmap.push({ date: today, messages: 0 });
                }

                if (userData.streak > 0 && userData.threshold > 0 && !userData.receivedDaily) {
                    updateFields.streak = 0;
                    updateFields.lastStreakLoss = new Date().toISOString();
                }

                await updateUserData(userId, updateFields);
            }
        }

        console.log("Manual daily reset completed successfully.");
    } catch (error) {
        console.error("Error during manual daily reset:", error);
    } finally {
        client.destroy();
    }
}

client.once('ready', async () => {
    console.log(`Logged in as ${client.user.tag}`);
    await resetDailyForAllUsers();
});

client.login(token);
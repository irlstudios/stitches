require('dotenv').config();
const { Client, GatewayIntentBits } = require('discord.js');
const { listUserData, updateUserData } = require('./dynamoDB');

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

        const users = await listUserData();
        for (const { userId } of users) {
            if (!userId) continue;

            const updateFields = {
                receivedDaily: false,
                threshold: 1
            };

            await updateUserData(userId, updateFields);
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
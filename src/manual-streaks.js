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

        if (!users || users.length === 0) {
            console.log("No users found in the database.");
            return;
        }

        console.log(`Total users to reset: ${users.length}`);

        for (const { userId, userData } of users) {
            if (!userId) {
                console.warn("Skipping entry due to missing userId.");
                continue;
            }

            console.log(`Updating user: ${userId}`);

            const updateFields = {
                receivedDaily: false,
                threshold: 1
            };

            try {
                await updateUserData(userId, updateFields);
                console.log(`✅ Successfully updated user: ${userId}`);
            } catch (updateError) {
                console.error(`❌ Error updating user ${userId}:`, updateError);
            }
        }

        console.log("✅ Manual daily reset completed successfully.");
    } catch (error) {
        console.error("❌ Error during manual daily reset:", error);
    } finally {
        console.log("Shutting down client...");
        client.destroy();
    }
}

client.once('ready', async () => {
    console.log(`Logged in as ${client.user.tag}`);
    await resetDailyForAllUsers();
});

client.login(token);
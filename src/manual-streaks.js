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

        console.log(`Total users retrieved: ${users.length}`);

        for (const userEntry of users) {
            if (!userEntry || typeof userEntry !== 'object') {
                console.warn("Skipping entry: Invalid structure", userEntry);
                continue;
            }

            const userId = userEntry.userId || userEntry.DiscordId;
            if (!userId || typeof userId !== 'string') {
                console.warn("Skipping entry due to missing or invalid userId:", userEntry);
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
const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
// Make sure this path is correct for your project structure
const { getUserData, updateUserData, listUserData } = require('../dynamoDB');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('resetmessages')
      .setDescription('MANUAL RESET: Resets weekly message counts for a user or the entire server.')
      .setDefaultMemberPermissions(PermissionsBitField.Flags.ManageGuild)
      .addSubcommand(subcommand =>
          subcommand
              .setName('user')
              .setDescription('Reset weekly message count for a specific user.')
              .addUserOption(option =>
                  option.setName('target')
                      .setDescription('The user to reset message count for')
                      .setRequired(true)))
      .addSubcommand(subcommand =>
          subcommand
              .setName('server')
              .setDescription('Reset weekly message counts for ALL users in the server.')),

  async execute(interaction) {
    const guildId = interaction.guild.id;
    const invokingUserId = interaction.user.id;
    const subcommand = interaction.options.getSubcommand();
    const commandId = interaction.id; // Unique ID for this interaction instance

    console.log(`[ResetMessages][${commandId}] Invoked by User ID: ${invokingUserId} in Guild ID: ${guildId}. Subcommand: ${subcommand}`);

    if (!interaction.memberPermissions?.has(PermissionsBitField.Flags.ManageGuild)) {
      console.log(`[ResetMessages][${commandId}] Permission check failed for User ID: ${invokingUserId} in Guild ID: ${guildId}. Missing 'ManageGuild'.`);
      return interaction.reply({ content: 'You must have the "Manage Server" permission to use this command.', ephemeral: true });
    }
    console.log(`[ResetMessages][${commandId}] Permission check passed for User ID: ${invokingUserId} in Guild ID: ${guildId}.`);

    try {
      console.log(`[ResetMessages][${commandId}] Attempting to defer reply for Guild ID: ${guildId}, Subcommand: ${subcommand}`);
      await interaction.deferReply({ ephemeral: true });
      console.log(`[ResetMessages][${commandId}] Reply deferred successfully for Guild ID: ${guildId}, Subcommand: ${subcommand}`);

      const resetFields = {
        messages: 0
      };
      console.log(`[ResetMessages][${commandId}] Reset fields defined: ${JSON.stringify(resetFields)} for Guild ID: ${guildId}, Subcommand: ${subcommand}`);

      if (subcommand === 'user') {
        const targetUser = interaction.options.getUser('target');
        const targetUserId = targetUser.id;
        console.log(`[ResetMessages][${commandId}] User subcommand selected. Target User ID: ${targetUserId}, Target Username: ${targetUser.username}, Guild ID: ${guildId}`);

        console.log(`[ResetMessages][${commandId}] Fetching user data for User ID: ${targetUserId}, Guild ID: ${guildId}`);
        const userData = await getUserData(guildId, targetUserId); // Assumes getUserData handles errors internally or throws them

        if (!userData) {
          console.log(`[ResetMessages][${commandId}] No data found via getUserData for User ID: ${targetUserId} in Guild ID: ${guildId}. No reset needed.`);
          return interaction.editReply({ content: `No data found for user ${targetUser.username} (${targetUserId}). No reset needed.` });
        }
        // Consider logging what userData contains if needed: console.log(`[ResetMessages][${commandId}] User data found:`, JSON.stringify(userData));
        console.log(`[ResetMessages][${commandId}] User data found for User ID: ${targetUserId} in Guild ID: ${guildId}. Proceeding with reset.`);

        console.log(`[ResetMessages][${commandId}] Attempting to update user data for User ID: ${targetUserId}, Guild ID: ${guildId} with fields: ${JSON.stringify(resetFields)}`);
        try {
          await updateUserData(guildId, targetUserId, resetFields);
          console.log(`[ResetMessages][${commandId}] Successfully updated user data via updateUserData for User ID: ${targetUserId}, Guild ID: ${guildId}`);
        } catch (updateError) {
          console.error(`[ResetMessages][${commandId}] Failed to update user data via updateUserData for User ID: ${targetUserId}, Guild ID: ${guildId}:`, updateError);
          // Let the main catch block handle the reply
          throw updateError; // Re-throw the error to be caught by the outer try-catch
        }

        const successMsg = `✅ Weekly message count for ${targetUser.username} (${targetUserId}) has been reset to 0.`;
        console.log(`[ResetMessages][${commandId}] Sending success reply for User ID: ${targetUserId}, Guild ID: ${guildId}. Message: "${successMsg}"`);
        await interaction.editReply({ content: successMsg });

      } else if (subcommand === 'server') {
        console.log(`[ResetMessages][${commandId}] Server subcommand selected for Guild ID: ${guildId}`);

        console.log(`[ResetMessages][${commandId}] Attempting to list all user data using listUserData for Guild ID: ${guildId}`);
        let users;
        try {
          users = await listUserData(guildId); // Assumes listUserData handles pagination and errors internally or throws them
          console.log(`[ResetMessages][${commandId}] listUserData returned ${users ? users.length : 'null/undefined'} users for Guild ID: ${guildId}.`);
          // Log the raw user list ONLY IF NEEDED for debugging small servers, can be very verbose!
          // if (users) { console.log(`[ResetMessages][${commandId}] Raw user list:`, JSON.stringify(users)); }
        } catch (listError) {
          console.error(`[ResetMessages][${commandId}] Failed to list user data via listUserData for Guild ID: ${guildId}:`, listError);
          // Let the main catch block handle the reply
          throw listError; // Re-throw the error
        }


        if (!users || users.length === 0) {
          console.log(`[ResetMessages][${commandId}] No users returned by listUserData for Guild ID: ${guildId}. No reset needed.`);
          return interaction.editReply({ content: 'No users found with data in this server (according to listUserData). No reset needed.' });
        }

        console.log(`[ResetMessages][${commandId}] Processing ${users.length} users found by listUserData in Guild ID: ${guildId}. Starting server-wide reset...`);
        let successfulUpdates = 0;
        let failedUpdates = 0;
        const updatePromises = []; // Store promises to run them concurrently

        for (const user of users) {
          // IMPORTANT: Check the structure of 'user' returned by listUserData.
          // Does it look like { userId: '123' } or just '123', or something else? Adjust accordingly.
          const userId = user.userId; // *** This assumes listUserData returns objects like { userId: '...', ... } ***

          if (!userId) {
            console.error(`[ResetMessages][${commandId}] Invalid user entry found during server reset loop in Guild ID: ${guildId}. Entry: ${JSON.stringify(user)}. Skipping this entry.`);
            failedUpdates++; // Count as failed because we couldn't process it
            continue; // Skip to the next user
          }

          console.log(`[ResetMessages][${commandId}] Preparing update promise for User ID: ${userId} in Guild ID: ${guildId}`);
          const promise = updateUserData(guildId, userId, resetFields)
              .then(() => {
                console.log(`[ResetMessages][${commandId}] Successfully reset messages for User ID: ${userId} in Guild ID: ${guildId}`);
                successfulUpdates++; // Increment success count here
              })
              .catch(err => {
                console.error(`[ResetMessages][${commandId}] Failed to reset messages via updateUserData for User ID: ${userId} in Guild ID: ${guildId}:`, err);
                failedUpdates++; // Increment failure count here
                // We don't re-throw here, Promise.all will still complete.
              });
          updatePromises.push(promise);
        }

        console.log(`[ResetMessages][${commandId}] Waiting for all ${updatePromises.length} user update promises to settle for Guild ID: ${guildId}.`);
        await Promise.all(updatePromises);
        console.log(`[ResetMessages][${commandId}] Finished processing all user updates for Guild ID: ${guildId}. Final counts - Success: ${successfulUpdates}, Failed: ${failedUpdates}, Total Processed: ${users.length}`);

        // Sanity check: Does successfulUpdates + failedUpdates match users.length?
        if (successfulUpdates + failedUpdates !== users.length) {
          console.warn(`[ResetMessages][${commandId}] Discrepancy detected! successfulUpdates (${successfulUpdates}) + failedUpdates (${failedUpdates}) does not equal total users processed (${users.length}) for Guild ID: ${guildId}. Check loop logic and userId extraction.`);
        }

        const totalUsersAttempted = users.length; // Use the count returned by listUserData
        const finalMsg = `✅ Server reset complete for Guild ID: ${guildId}. Attempted resets for ${totalUsersAttempted} users found. Successful: ${successfulUpdates}. Failed: ${failedUpdates}.`;
        console.log(`[ResetMessages][${commandId}] Sending final reply for server reset in Guild ID: ${guildId}. Message: "${finalMsg}"`);
        await interaction.editReply({ content: finalMsg });
        console.log(`[ResetMessages][${commandId}] Finished server reset command execution successfully for Guild ID: ${guildId}.`);
      }

    } catch (error) {
      // Log the specific subcommand context in the main error handler
      console.error(`[ResetMessages][${commandId}] CRITICAL ERROR during execution of subcommand '${subcommand}' in Guild ID: ${guildId} by User ID: ${invokingUserId}:`, error);
      const errorMsg = `An error occurred while executing the '${subcommand}' reset. Please check the bot logs for details (Interaction ID: ${commandId}).`;
      try {
        if (interaction.deferred || interaction.replied) {
          console.log(`[ResetMessages][${commandId}] Attempting to edit reply with error message for Guild ID: ${guildId}.`);
          await interaction.editReply({ content: errorMsg });
        } else {
          // Should not happen if we deferred, but as a fallback
          console.log(`[ResetMessages][${commandId}] Attempting to send initial reply with error message for Guild ID: ${guildId}.`);
          await interaction.reply({ content: errorMsg, ephemeral: true });
        }
        console.log(`[ResetMessages][${commandId}] Error message sent to user for Guild ID: ${guildId}.`);
      } catch (replyError) {
        // Log error during error reporting
        console.error(`[ResetMessages][${commandId}] FATAL: Failed to send error reply back to Discord for Guild ID: ${guildId}:`, replyError);
        console.error(`[ResetMessages][${commandId}] Original error that triggered this was:`, error); // Log original error again
      }
    }
  }
};
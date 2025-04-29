const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
const { getUserData, updateUserData, listUserData, batchResetMessageAttributes, updatePrimaryUserMessages } = require('../dynamoDB'); // Added batchResetMessageAttributes and updatePrimaryUserMessages

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
    const commandId = interaction.id;

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
      const resetValue = 0;
      console.log(`[ResetMessages][${commandId}] Reset value defined: ${resetValue} for Guild ID: ${guildId}, Subcommand: ${subcommand}`);

      if (subcommand === 'user') {
        const targetUser = interaction.options.getUser('target');
        const targetUserId = targetUser.id;
        console.log(`[ResetMessages][${commandId}] User subcommand selected. Target User ID: ${targetUserId}, Target Username: ${targetUser.username}, Guild ID: ${guildId}`);

        console.log(`[ResetMessages][${commandId}] Fetching user data for User ID: ${targetUserId}, Guild ID: ${guildId}`);
        const userData = await getUserData(guildId, targetUserId);

        if (!userData) {
          console.log(`[ResetMessages][${commandId}] No data found via getUserData for User ID: ${targetUserId} in Guild ID: ${guildId}. No reset needed.`);
          return interaction.editReply({ content: `No data found for user ${targetUser.username} (${targetUserId}). No reset needed.` });
        }
        console.log(`[ResetMessages][${commandId}] User data found for User ID: ${targetUserId} in Guild ID: ${guildId}. Proceeding with reset.`);

        console.log(`[ResetMessages][${commandId}] Attempting to update user data for User ID: ${targetUserId}, Guild ID: ${guildId} with fields: ${JSON.stringify(resetFields)}`);
        try {

          await updateUserData(guildId, targetUserId, resetFields);
          console.log(`[ResetMessages][${commandId}] Successfully updated user data (primary & attribute) via updateUserData for User ID: ${targetUserId}, Guild ID: ${guildId}`);
        } catch (updateError) {
          console.error(`[ResetMessages][${commandId}] Failed to update user data via updateUserData for User ID: ${targetUserId}, Guild ID: ${guildId}:`, updateError);

          throw updateError;
        }

        const successMsg = `✅ Weekly message count for ${targetUser.username} (${targetUserId}) has been reset to 0.`;
        console.log(`[ResetMessages][${commandId}] Sending success reply for User ID: ${targetUserId}, Guild ID: ${guildId}. Message: "${successMsg}"`);
        await interaction.editReply({ content: successMsg });

      } else if (subcommand === 'server') {
        console.log(`[ResetMessages][${commandId}] Server subcommand selected for Guild ID: ${guildId}`);

        console.log(`[ResetMessages][${commandId}] Attempting to list all user data using listUserData for Guild ID: ${guildId}`);
        let users;
        try {
          users = await listUserData(guildId);
          console.log(`[ResetMessages][${commandId}] listUserData returned ${users ? users.length : 'null/undefined'} users for Guild ID: ${guildId}.`);

        } catch (listError) {
          console.error(`[ResetMessages][${commandId}] Failed to list user data via listUserData for Guild ID: ${guildId}:`, listError);

          throw listError;
        }


        if (!users || users.length === 0) {
          console.log(`[ResetMessages][${commandId}] No users returned by listUserData for Guild ID: ${guildId}. No reset needed.`);
          return interaction.editReply({ content: 'No users found with data in this server (according to listUserData). No reset needed.' });
        }

        const userIdsToReset = users.map(user => user.userId).filter(id => id); // Extract valid user IDs
        const totalUsersAttempted = userIdsToReset.length;
        console.log(`[ResetMessages][${commandId}] Extracted ${totalUsersAttempted} valid user IDs from ${users.length} records in Guild ID: ${guildId}. Starting server-wide reset...`);

        if (totalUsersAttempted === 0) {
          console.log(`[ResetMessages][${commandId}] No valid user IDs found after filtering. No reset needed.`);
          return interaction.editReply({ content: 'No users with valid IDs found in the data. No reset performed.' });
        }

        // --- Step 1: Batch Reset GSI Attribute Items ---
        console.log(`[ResetMessages][${commandId}] Step 1: Starting batch reset for 'messages' attribute items for ${totalUsersAttempted} users in Guild ID: ${guildId}.`);
        let batchResult = { success: false, successCount: 0, failCount: totalUsersAttempted }; // Default to failure
        try {
          batchResult = await batchResetMessageAttributes(guildId, userIdsToReset);
          console.log(`[ResetMessages][${commandId}] Step 1: Batch reset for attribute items completed for Guild ID: ${guildId}. Success: ${batchResult.success}, Succeeded: ${batchResult.successCount}, Failed: ${batchResult.failCount}.`);
        } catch (batchError) {
          console.error(`[ResetMessages][${commandId}] Step 1: CRITICAL Error during batchResetMessageAttributes for Guild ID: ${guildId}:`, batchError);

          throw batchError; // Halt execution if the batch function itself throws catastrophically
        }

        // --- Step 2: Update Primary User Records Individually ---
        console.log(`[ResetMessages][${commandId}] Step 2: Starting individual updates for primary user records (userData.messages) for ${totalUsersAttempted} users in Guild ID: ${guildId}.`);
        let primaryUpdateSuccessCount = 0;
        let primaryUpdateFailCount = 0;
        const primaryUpdatePromises = [];

        for (const userId of userIdsToReset) {
          console.log(`[ResetMessages][${commandId}] Preparing primary update promise for User ID: ${userId} in Guild ID: ${guildId}`);
          const promise = updatePrimaryUserMessages(guildId, userId, resetValue)
              .then(result => {
                if(result.success) {
                  console.log(`[ResetMessages][${commandId}] Successfully updated primary messages for User ID: ${userId} in Guild ID: ${guildId}`);
                  primaryUpdateSuccessCount++;
                } else {
                  console.error(`[ResetMessages][${commandId}] Failed to update primary messages for User ID: ${userId} in Guild ID: ${guildId}:`, result.error);
                  primaryUpdateFailCount++;
                }
              })
              .catch(err => {
                // This catch is primarily for unexpected errors from the promise creation/setup itself
                console.error(`[ResetMessages][${commandId}] Unexpected error setting up/awaiting primary update for User ID: ${userId} in Guild ID: ${guildId}:`, err);
                primaryUpdateFailCount++;
              });
          primaryUpdatePromises.push(promise);
        }

        console.log(`[ResetMessages][${commandId}] Waiting for all ${primaryUpdatePromises.length} primary user update promises to settle for Guild ID: ${guildId}.`);
        await Promise.all(primaryUpdatePromises);
        console.log(`[ResetMessages][${commandId}] Finished processing all primary user updates for Guild ID: ${guildId}. Final primary counts - Success: ${primaryUpdateSuccessCount}, Failed: ${primaryUpdateFailCount}.`);


        // --- Final Report ---
        const overallSuccess = batchResult.success && primaryUpdateFailCount === 0; // Consider overall success only if batch worked AND all primary updates worked
        let finalMsg = `✅ Server reset complete for Guild ID: ${guildId}.`;
        if (!overallSuccess) {
          finalMsg = `⚠️ Server reset partially completed for Guild ID: ${guildId}.`;
        }

        finalMsg += `\nAttempted resets for ${totalUsersAttempted} users found.`;
        finalMsg += `\nLeaderboard Attribute Updates: ${batchResult.successCount} succeeded, ${batchResult.failCount} failed.`;
        finalMsg += `\nPrimary Record Updates: ${primaryUpdateSuccessCount} succeeded, ${primaryUpdateFailCount} failed.`;

        if (!overallSuccess) {
          finalMsg += `\nPlease check the bot logs (Interaction ID: ${commandId}) for details on failures. Leaderboard might be inconsistent.`;
        }

        console.log(`[ResetMessages][${commandId}] Sending final reply for server reset in Guild ID: ${guildId}. Message: "${finalMsg}"`);
        await interaction.editReply({ content: finalMsg });
        console.log(`[ResetMessages][${commandId}] Finished server reset command execution for Guild ID: ${guildId}. Overall Success: ${overallSuccess}`);
      }

    } catch (error) {

      console.error(`[ResetMessages][${commandId}] CRITICAL ERROR during execution of subcommand '${subcommand}' in Guild ID: ${guildId} by User ID: ${invokingUserId}:`, error);
      const errorMsg = `An error occurred while executing the '${subcommand}' reset. Please check the bot logs for details (Interaction ID: ${commandId}).`;
      try {
        if (interaction.deferred || interaction.replied) {
          console.log(`[ResetMessages][${commandId}] Attempting to edit reply with error message for Guild ID: ${guildId}.`);
          await interaction.editReply({ content: errorMsg });
        } else {

          console.log(`[ResetMessages][${commandId}] Attempting to send initial reply with error message for Guild ID: ${guildId}.`);
          await interaction.reply({ content: errorMsg, ephemeral: true });
        }
        console.log(`[ResetMessages][${commandId}] Error message sent to user for Guild ID: ${guildId}.`);
      } catch (replyError) {

        console.error(`[ResetMessages][${commandId}] FATAL: Failed to send error reply back to Discord for Guild ID: ${guildId}:`, replyError);
        console.error(`[ResetMessages][${commandId}] Original error that triggered this was:`, error);
      }
    }
  }
};
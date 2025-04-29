const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
const { getUserData, updateUserData, listUserData, batchResetMessageAttributes, updatePrimaryUserMessages } = require('../dynamoDB');

async function processInChunks(items, chunkSize, asyncOperation) {
  let results = [];
  console.log(`[Chunk Processor] Starting processing of ${items.length} items in chunks of ${chunkSize}.`);
  for (let i = 0; i < items.length; i += chunkSize) {
    const chunk = items.slice(i, i + chunkSize);
    const chunkNumber = Math.floor(i / chunkSize) + 1;
    const totalChunks = Math.ceil(items.length / chunkSize);
    console.log(`[Chunk Processor] Processing chunk ${chunkNumber}/${totalChunks} (Size: ${chunk.length}).`);
    const chunkStartTime = Date.now();
    try {
      const chunkPromises = chunk.map(item => asyncOperation(item));
      const chunkResults = await Promise.all(chunkPromises);
      results = results.concat(chunkResults);
      const chunkEndTime = Date.now();
      console.log(`[Chunk Processor] Finished chunk ${chunkNumber}/${totalChunks}. Duration: ${chunkEndTime - chunkStartTime}ms.`);
    } catch (error) {
      console.error(`[Chunk Processor] CRITICAL Error processing chunk ${chunkNumber}/${totalChunks}:`, error);

      const failedResults = chunk.map(item => ({ success: false, item: item, error: new Error("Chunk processing failed") }));
      results = results.concat(failedResults);
    }

  }
  console.log(`[Chunk Processor] Finished processing all chunks.`);
  return results;
}


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
          console.log(`[ResetMessages][${commandId}] updateUserData call completed for User ID: ${targetUserId}, Guild ID: ${guildId}. Check previous logs for success/failure details.`);
          const successMsg = `✅ Weekly message count for ${targetUser.username} (${targetUserId}) has been reset. Check leaderboard shortly.`;
          console.log(`[ResetMessages][${commandId}] Sending success reply for User ID: ${targetUserId}, Guild ID: ${guildId}. Message: "${successMsg}"`);
          await interaction.editReply({ content: successMsg });

        } catch (updateError) {
          console.error(`[ResetMessages][${commandId}] updateUserData call failed for User ID: ${targetUserId}, Guild ID: ${guildId}:`, updateError);

          throw updateError;
        }

      } else if (subcommand === 'server') {
        const serverResetStartTime = Date.now();
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
          return interaction.editReply({ content: 'No users found with data in this server. No reset needed.' });
        }

        const userIdsToReset = users.map(user => user.userId).filter(id => id);
        const totalUsersAttempted = userIdsToReset.length;
        console.log(`[ResetMessages][${commandId}] Extracted ${totalUsersAttempted} valid user IDs from ${users.length} records in Guild ID: ${guildId}. Starting server-wide reset...`);

        if (totalUsersAttempted === 0) {
          console.log(`[ResetMessages][${commandId}] No valid user IDs found after filtering. No reset needed.`);
          return interaction.editReply({ content: 'No users with valid IDs found in the data. No reset performed.' });
        }


        console.log(`[ResetMessages][${commandId}] Step 1: Starting batch reset for 'messages' attribute items for ${totalUsersAttempted} users in Guild ID: ${guildId}.`);
        let batchResult = { success: false, successCount: 0, failCount: totalUsersAttempted };
        try {
          batchResult = await batchResetMessageAttributes(guildId, userIdsToReset);
          console.log(`[ResetMessages][${commandId}] Step 1: Batch reset for attribute items completed for Guild ID: ${guildId}. Overall Success: ${batchResult.success}, Succeeded: ${batchResult.successCount}, Failed: ${batchResult.failCount}.`);
        } catch (batchError) {
          console.error(`[ResetMessages][${commandId}] Step 1: CRITICAL Error during batchResetMessageAttributes for Guild ID: ${guildId}:`, batchError);

          throw batchError;
        }


        console.log(`[ResetMessages][${commandId}] Step 2: Starting chunked individual updates for primary user records (userData.messages) for ${totalUsersAttempted} users in Guild ID: ${guildId}.`);
        const PRIMARY_UPDATE_CHUNK_SIZE = 20;
        let primaryUpdateResults = [];
        try {
          primaryUpdateResults = await processInChunks(userIdsToReset, PRIMARY_UPDATE_CHUNK_SIZE, async (userId) => {

            console.log(`[ResetMessages][${commandId}][ChunkWorker] Updating primary messages for User ID: ${userId}...`);
            return await updatePrimaryUserMessages(guildId, userId, resetValue);
          });
        } catch (chunkError) {
          console.error(`[ResetMessages][${commandId}] Step 2: CRITICAL Error during chunk processing for primary updates in Guild ID: ${guildId}:`, chunkError);

        }

        const primaryUpdateSuccessCount = primaryUpdateResults.filter(r => r.success).length;
        const primaryUpdateFailCount = primaryUpdateResults.filter(r => !r.success).length;
        console.log(`[ResetMessages][${commandId}] Finished processing all primary user update chunks for Guild ID: ${guildId}. Final primary counts - Success: ${primaryUpdateSuccessCount}, Failed: ${primaryUpdateFailCount}. Total results received: ${primaryUpdateResults.length}`);


        if (primaryUpdateResults.length !== totalUsersAttempted) {
          console.warn(`[ResetMessages][${commandId}] Discrepancy detected! Number of primary update results (${primaryUpdateResults.length}) does not match total users attempted (${totalUsersAttempted}). Failures might be undercounted.`);

        }



        const serverResetEndTime = Date.now();
        const overallSuccess = batchResult.success && primaryUpdateFailCount === 0;
        let finalMsg = `✅ Server reset command finished processing for Guild ID: ${guildId}.`;
        if (!overallSuccess) {
          finalMsg = `⚠️ Server reset command finished with potential issues for Guild ID: ${guildId}.`;
        }

        finalMsg += `\nDuration: ${((serverResetEndTime - serverResetStartTime) / 1000).toFixed(2)} seconds.`;
        finalMsg += `\nAttempted resets for ${totalUsersAttempted} users found.`;
        finalMsg += `\nLeaderboard Attribute Updates: ${batchResult.successCount} succeeded, ${batchResult.failCount} failed.`;
        finalMsg += `\nPrimary Record Updates: ${primaryUpdateSuccessCount} succeeded, ${primaryUpdateFailCount} failed.`;

        if (!overallSuccess) {
          finalMsg += `\nPlease check the bot logs (Interaction ID: ${commandId}) for details on failures. Leaderboard or user data might be inconsistent.`;
        } else {
          finalMsg += `\nLeaderboard should update shortly reflecting the reset.`;
        }

        console.log(`[ResetMessages][${commandId}] Sending final reply for server reset in Guild ID: ${guildId}. Message: "${finalMsg}"`);
        await interaction.editReply({ content: finalMsg });
        console.log(`[ResetMessages][${commandId}] Finished server reset command execution for Guild ID: ${guildId}. Overall Success: ${overallSuccess}. Duration: ${serverResetEndTime - serverResetStartTime}ms`);
      }

    } catch (error) {

      console.error(`[ResetMessages][${commandId}] CRITICAL ERROR during execution of subcommand '${subcommand}' in Guild ID: ${guildId} by User ID: ${invokingUserId}:`, error);
      const errorMsg = `An error occurred while executing the '${subcommand}' reset. The operation likely failed. Please check the bot logs for details (Interaction ID: ${commandId}).`;
      try {

        if (interaction.channel && interaction.replied !== true) {
          console.log(`[ResetMessages][${commandId}] Interaction was not replied, attempting initial reply with error message.`);
          await interaction.reply({ content: errorMsg, ephemeral: true });
        } else if (interaction.editable) {
          console.log(`[ResetMessages][${commandId}] Attempting to edit reply with error message.`);
          await interaction.editReply({ content: errorMsg, components: [], embeds: [] });
        } else {
          console.log(`[ResetMessages][${commandId}] Could not edit reply or reply initially, attempting followup.`);
          await interaction.followUp({ content: errorMsg, ephemeral: true });
        }
        console.log(`[ResetMessages][${commandId}] Error message sent to user for Guild ID: ${guildId}.`);
      } catch (replyError) {

        console.error(`[ResetMessages][${commandId}] FATAL: Failed to send error reply back to Discord for Guild ID: ${guildId}:`, replyError);
        console.error(`[ResetMessages][${commandId}] Original error that triggered this was:`, error);
      }
    }
  }
};
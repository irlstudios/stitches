'use strict';
const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
const {
  getUserData,
  updateUserData,
  listUserData,
  batchResetMessageAttributes,
  updatePrimaryUserMessages
} = require('../dynamoDB');

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
      results = results.concat(chunkResults.filter(r => r !== undefined));
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
      console.log(`[ResetMessages][${commandId}] Permission check failed for User ID: ${invokingUserId} in Guild ID: ${guildId}.`);
      return interaction.reply({ content: 'You must have the "Manage Server" permission to use this command.', ephemeral: true });
    }
    console.log(`[ResetMessages][${commandId}] Permission check passed.`);
    try {
      await interaction.deferReply({ ephemeral: true });
      const resetFields = { messages: 0 };
      const resetValue = 0;
      console.log(`[ResetMessages][${commandId}] Reset value defined: ${resetValue}`);
      if (subcommand === 'user') {
        const targetUser = interaction.options.getUser('target');
        const targetUserId = targetUser.id;
        console.log(`[ResetMessages][${commandId}] User subcommand: Target User ID: ${targetUserId}`);
        const userData = await getUserData(guildId, targetUserId);
        if (!userData) {
          return interaction.editReply({ content: `No data found for user ${targetUser.username} (${targetUserId}). No reset needed.` });
        }
        await updateUserData(guildId, targetUserId, resetFields);
        const successMsg = `✅ Weekly message count for ${targetUser.username} (${targetUserId}) has been reset.`;
        return interaction.editReply({ content: successMsg });
      } else if (subcommand === 'server') {
        const serverResetStartTime = Date.now();
        const users = await listUserData(guildId);
        if (!users.length) {
          return interaction.editReply({ content: 'No users found with data in this server. No reset needed.' });
        }
        const userIdsToReset = users.map(u => u.userId).filter(id => id);
        if (!userIdsToReset.length) {
          return interaction.editReply({ content: 'No users with valid IDs found in the data. No reset performed.' });
        }
        const batchResult = await batchResetMessageAttributes(guildId, userIdsToReset);
        console.log(`[ResetMessages][${commandId}] Batch reset attributes: ${batchResult.successCount}/${batchResult.processedCount}`);
        const primaryUpdateResults = await processInChunks(
            userIdsToReset,
            20,
            async userId => {
              console.log(`[ResetMessages][${commandId}][ChunkWorker] Updating primary messages for User ID: ${userId}...`);
              return await updatePrimaryUserMessages(guildId, userId, resetValue);
            }
        );
        const primarySuccess = primaryUpdateResults.filter(r => r && r.success).length;
        const primaryFail = primaryUpdateResults.filter(r => r && !r.success).length;
        const serverResetEndTime = Date.now();
        const overallSuccess = batchResult.success && primaryFail === 0;
        let finalMsg = overallSuccess
            ? `✅ Server reset finished successfully.`
            : `⚠️ Server reset finished with issues.`;
        finalMsg += `\nDuration: ${((serverResetEndTime - serverResetStartTime) / 1000).toFixed(2)}s.`;
        finalMsg += `\nAttribute resets: ${batchResult.successCount}/${batchResult.processedCount}.`;
        finalMsg += `\nPrimary updates: ${primarySuccess}/${primaryUpdateResults.length}.`;
        return interaction.editReply({ content: finalMsg });
      }
    } catch (error) {
      console.error(`[ResetMessages][${commandId}] CRITICAL ERROR:`, error);
      const errorMsg = `An error occurred executing '${subcommand}' reset. (ID: ${commandId})`;
      try {
        return interaction.editReply({ content: errorMsg });
      } catch {
        return;
      }
    }
  }
};
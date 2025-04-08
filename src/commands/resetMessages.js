const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
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
    if (!interaction.memberPermissions?.has(PermissionsBitField.Flags.ManageGuild)) {
      return interaction.reply({ content: 'You must have the "Manage Server" permission to use this command.', ephemeral: true });
    }

    const guildId = interaction.guild.id;
    const subcommand = interaction.options.getSubcommand();

    try {
      await interaction.deferReply({ ephemeral: true });

      const resetFields = {
        messages: 0
      };

      if (subcommand === 'user') {
        const targetUser = interaction.options.getUser('target');

        const userData = await getUserData(guildId, targetUser.id);
        if (!userData) {
          return interaction.editReply({ content: `No data found for user ${targetUser.username}. No reset needed.` });
        }

        await updateUserData(guildId, targetUser.id, resetFields);
        await interaction.editReply({ content: `✅ Weekly message count for ${targetUser.username} has been reset to 0.` });

      } else if (subcommand === 'server') {
        const users = await listUserData(guildId);
        if (users.length === 0) {
          return interaction.editReply({ content: 'No users found with data in this server. No reset needed.' });
        }

        console.log(`[ResetMessages] Starting server reset for ${users.length} users in guild ${guildId}...`);
        const updatePromises = users.map(({ userId }) =>
            updateUserData(guildId, userId, resetFields)
                .catch(err => {
                  console.error(`[ResetMessages] Failed to reset messages for user ${userId} during server reset in guild ${guildId}:`, err);
                  return null;
                })
        );

        await Promise.all(updatePromises);

        await interaction.editReply({ content: `✅ Weekly message counts for all ${users.length} users in the server have been reset to 0.` });
        console.log(`[ResetMessages] Finished server reset for guild ${guildId}.`);
      }

    } catch (error) {
      console.error(`Failed to execute resetmessages command (subcommand: ${subcommand}) in guild ${guildId}:`, error);
      const errorMsg = 'An error occurred while trying to reset message counts.';
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({ content: errorMsg }).catch(console.error);
      } else {
        await interaction.reply({ content: errorMsg, ephemeral: true }).catch(console.error);
      }
    }
  }
};
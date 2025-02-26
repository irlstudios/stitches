const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
const { getUserData, saveUserData, listUserData } = require('../dynamoDB');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('resetmessages')
    .setDescription('Reset message counts for a user or the entire server.')
    .addSubcommand(subcommand =>
      subcommand
        .setName('user')
        .setDescription('Reset message count for a specific user.')
        .addUserOption(option =>
          option.setName('target')
            .setDescription('The user to reset message count for')
            .setRequired(true)))
    .addSubcommand(subcommand =>
      subcommand
        .setName('server')
        .setDescription('Reset message counts for all users in the server.')),
  async execute(interaction) {
    if (!interaction.member.permissions.has(PermissionsBitField.Flags.ManageGuild)) {
      return interaction.reply({ content: 'You do not have permission to use this command.', ephemeral: true });
    }
    const guildId = interaction.guild.id;
    try {
      if (interaction.options.getSubcommand() === 'user') {
        const target = interaction.options.getUser('target');
        let userData = await getUserData(guildId, target.id);
        if (!userData) {
          return interaction.reply({ content: `No data found for user ${target.username}.`, ephemeral: true });
        }
        userData.messages = 0;
        userData.totalMessages = 0;
        userData.highestMessageCount = 0;
        userData.messageHeatmap = [];
        await saveUserData(guildId, target.id, userData);
        return interaction.reply({ content: `Message count for ${target.username} has been reset.`, ephemeral: true });
      } else if (interaction.options.getSubcommand() === 'server') {
        const users = await listUserData(guildId);
        for (const { userId, userData } of users) {
          userData.messages = 0;
          userData.totalMessages = 0;
          userData.highestMessageCount = 0;
          userData.messageHeatmap = [];
          await saveUserData(guildId, userId, userData);
        }
        return interaction.reply({ content: 'Message counts for all users in the server have been reset.', ephemeral: true });
      }
    } catch (error) {
      console.error(`Failed to reset message counts: ${error.message}`);
      return interaction.reply({ content: 'An error occurred while trying to reset message counts. Please try again later.', ephemeral: true });
    }
  },
};
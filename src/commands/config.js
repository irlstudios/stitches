const { SlashCommandBuilder, ActionRowBuilder, StringSelectMenuBuilder, PermissionsBitField } = require('discord.js');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('stitches-configuration')
    .setDescription('Configure the streak system, message leader system, or level system.'),
  async execute(interaction) {
    try {
      if (!interaction.member.permissions.has(PermissionsBitField.Flags.ManageGuild)) {
        return await interaction.reply({
          content: 'You do not have permission to use this command.',
          ephemeral: true,
        });
      }
      const selectMenu = new StringSelectMenuBuilder()
        .setCustomId('system-select')
        .setPlaceholder('Select the system to configure')
        .addOptions([
          { label: 'Streak System', value: 'streakSystem' },
          { label: 'Message Leader System', value: 'messageLeaderSystem' },
          { label: 'Level System', value: 'levelSystem' },
          { label: 'Analytics System', value: 'weeklyReportSystem' }
        ]);
      const row = new ActionRowBuilder().addComponents(selectMenu);
      await interaction.reply({
        content: 'Please select the system you want to configure:',
        components: [row],
        ephemeral: true,
      });
    } catch (error) {
      console.error(`Error executing the configuration command: ${error.message}`);
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({
          content: 'An error occurred while executing the command.',
          ephemeral: true,
        });
      }
    }
  },
};
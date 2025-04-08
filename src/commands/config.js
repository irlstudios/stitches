const { SlashCommandBuilder, ActionRowBuilder, StringSelectMenuBuilder, PermissionsBitField } = require('discord.js');

module.exports = {
    data: new SlashCommandBuilder()
        .setName('stitches-configuration')
        .setDescription('Configure the streak, message leader, level, and analytics systems along with their metrics.')
        .setDefaultMemberPermissions(PermissionsBitField.Flags.ManageGuild),
    async execute(interaction) {
        try {
            if (!interaction.memberPermissions?.has(PermissionsBitField.Flags.ManageGuild)) {
                return await interaction.reply({ content: 'You must have the "Manage Server" permission to use this command.', ephemeral: true });
            }

            if (!interaction.guild) {
                return await interaction.reply({ content: 'This command can only be used within a server.', ephemeral: true });
            }


            const selectMenu = new StringSelectMenuBuilder()
                .setCustomId('system-select')
                .setPlaceholder('Select the system to configure')
                .addOptions([
                    { label: 'Streak System', value: 'streakSystem', description: 'Configure message streaks, roles, and threshold.' },
                    { label: 'Message Leader System', value: 'messageLeaderSystem', description: 'Configure weekly message leader announcements.' },
                    { label: 'Level System', value: 'levelSystem', description: 'Configure XP gain, levels, and roles.' },
                    { label: 'Analytics System', value: 'reportSettings', description: 'Configure channels for weekly/monthly reports.' }
                ]);

            const row = new ActionRowBuilder().addComponents(selectMenu);

            await interaction.reply({
                content: 'Please select the bot system you want to configure from the menu below:',
                components: [row],
                ephemeral: true
            });

        } catch (error) {
            console.error(`Error executing the configuration command in guild ${interaction.guildId}:`, error);
            const errorMessage = 'An error occurred while starting the configuration process.';
            try {
                if (!interaction.replied && !interaction.deferred) {
                    await interaction.reply({ content: errorMessage, ephemeral: true });
                } else {
                    await interaction.followUp({ content: errorMessage, ephemeral: true });
                }
            } catch (replyError) {
                console.error("Failed to send error reply for configuration command:", replyError);
            }
        }
    }
};

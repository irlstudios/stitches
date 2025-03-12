const { SlashCommandBuilder, EmbedBuilder, PermissionsBitField } = require('discord.js');
const { getUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('debug')
      .setDescription('Sends debug information to the developers and provides a link to the user.')
      .addUserOption(option =>
          option.setName('target')
              .setDescription('The user to debug')
              .setRequired(false)),
  async execute(interaction) {
    const { guild, user, client } = interaction;
    const developerChannelId = '1280248284358774784';
    const developerGuildId = '1233740086839869501';

    try {
      const targetUser = interaction.options.getUser('target') || user;
      const member = guild.members.cache.get(targetUser.id);

      const guildConfig = await getConfig(guild.id) || {};
      const userData = await getUserData(guild.id, targetUser.id) || {};

      const channelPermissions = interaction.channel.permissionsFor(client.user);
      const botMember = await guild.members.fetch(client.user.id);
      const botPermissions = botMember.permissions;

      const requiredPermissions = [
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.EmbedLinks,
        PermissionsBitField.Flags.AttachFiles,
        PermissionsBitField.Flags.ManageRoles,
        PermissionsBitField.Flags.ManageChannels,
        PermissionsBitField.Flags.ReadMessageHistory,
        PermissionsBitField.Flags.ViewChannel,
      ];

      const check = '✅';
      const cross = '❌';

      const botPermsDisplay = requiredPermissions.map(perm =>
          botPermissions.has(perm)
              ? `${check} ${perm.toString().replace(/_/g, ' ')}`
              : `${cross} ${perm.toString().replace(/_/g, ' ')}`
      ).join('\n');

      const channelPermsDisplay = requiredPermissions.map(perm =>
          channelPermissions.has(perm)
              ? `${check} ${perm.toString().replace(/_/g, ' ')}`
              : `${cross} ${perm.toString().replace(/_/g, ' ')}`
      ).join('\n');

      const filteredUserData = {
        streak: userData.streak,
        highestStreak: userData.highestStreak,
        messages: userData.messages,
        threshold: userData.threshold,
        receivedDaily: userData.receivedDaily,
        messageLeaderWins: userData.messageLeaderWins,
        daysTracked: userData.daysTracked,
        averageMessagesPerDay: userData.averageMessagesPerDay,
        activeDaysCount: userData.activeDaysCount,
        lastStreakLoss: userData.lastStreakLoss,
        experience: userData.experience,
        boosters: userData.boosters,
        expireAt: userData.expireAt,
        lastUpdated: userData.lastUpdated,
      };

      const devEmbed = new EmbedBuilder()
          .setColor('#FF0000')
          .setTitle('Debug Information')
          .setDescription(`Debug information for ${targetUser.tag}`)
          .addFields(
              {
                name: '**Server Information**',
                value: `**Server Name:** ${guild.name}\n**Server ID:** ${guild.id}\n**Member Count:** ${guild.memberCount}`,
                inline: true
              },
              {
                name: '**User Information**',
                value: `**User Name:** ${targetUser.tag}\n**User ID:** ${targetUser.id}\n**Roles:** ${member.roles.cache.map(role => role.name).join(', ')}`,
                inline: true
              },
              { name: '**Bot Permissions**', value: botPermsDisplay, inline: false },
              { name: '**Channel Permissions**', value: channelPermsDisplay, inline: false },
              {
                name: '**Guild Configuration**',
                value: `\`\`\`json\n${JSON.stringify(guildConfig, null, 2)}\n\`\`\``,
                inline: false
              },
              {
                name: '**User Data**',
                value: `\`\`\`json\n${JSON.stringify(filteredUserData, null, 2)}\n\`\`\``,
                inline: false
              }
          )
          .setTimestamp();

      const devGuild = client.guilds.cache.get(developerGuildId);
      const devChannel = devGuild.channels.cache.get(developerChannelId);
      const debugMessage = await devChannel.send({ embeds: [devEmbed] });

      const userEmbed = new EmbedBuilder()
          .setColor('#00FF00')
          .setTitle('Debug Information Sent')
          .setDescription(`If support requested this debug, give them this link: https://discord.com/channels/${developerGuildId}/${developerChannelId}/${debugMessage.id}`)
          .addFields(
              { name: 'Bot Permissions', value: botPermsDisplay, inline: true },
              { name: 'Channel Permissions', value: channelPermsDisplay, inline: true },
              {
                name: 'Server Information',
                value: `**Server Name:** ${guild.name}\n**Server ID:** ${guild.id}\n**Member Count:** ${guild.memberCount}`,
                inline: false
              },
              {
                name: 'User Information',
                value: `**User Name:** ${targetUser.tag}\n**User ID:** ${targetUser.id}`,
                inline: false
              },
              {
                name: 'Bot Information',
                value: `**Bot Name:** ${client.user.tag}\n**Bot ID:** ${client.user.id}`,
                inline: false
              }
          )
          .setTimestamp();

      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({ embeds: [userEmbed], ephemeral: true });
      }
    } catch (error) {
      console.error(`Error sending debug information: ${error.message}`);
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({ content: 'An error occurred while sending debug information.', ephemeral: true });
      }
    }
  },
};
const { SlashCommandBuilder, EmbedBuilder, PermissionsBitField } = require('discord.js');
const { getUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('debug')
      .setDescription('Sends diagnostic information to developers.')
      .setDefaultMemberPermissions(PermissionsBitField.Flags.ManageGuild) // Recommended permission
      .addUserOption(option =>
          option.setName('target')
              .setDescription('The user to get debug info for (defaults to you)')
              .setRequired(false)),

  async execute(interaction) {
    const { guild, user: interactionUser, client } = interaction;
    if (!guild) {
      return interaction.reply({ content: "This command can only be used in a server.", ephemeral: true });
    }

    const developerChannelId = '1280248284358774784';
    const developerGuildId = '1233740086839869501';

    if (!developerChannelId || !developerGuildId || developerChannelId === 'YOUR_DEVELOPER_CHANNEL_ID') {
      console.error("Developer Channel/Guild ID not configured in debug command.");
      return interaction.reply({ content: "Debug command is not configured correctly by the bot owner.", ephemeral: true });
    }


    const targetUser = interaction.options.getUser('target') || interactionUser;
    const targetMember = await guild.members.fetch(targetUser.id).catch(() => null);

    if (!targetMember) {
      const replyMethod = interaction.deferred ? interaction.editReply : interaction.reply;
      return replyMethod.call(interaction, { content: `Could not find the target user ${targetUser.tag} in this server.`, ephemeral: true });
    }

    try {
      await interaction.deferReply({ ephemeral: true });

      const guildConfig = await getConfig(guild.id) || {};

      const rawUserData = await getUserData(guild.id, targetUser.id);

      const botMember = guild.members.me;
      if (!botMember) {
        return interaction.editReply({ content: "Could not fetch bot's member information.", ephemeral: true });
      }
      const channelPermissions = interaction.channel.permissionsFor(botMember);
      const botPermissions = botMember.permissions;

      const requiredPermissions = [
        PermissionsBitField.Flags.SendMessages,
        PermissionsBitField.Flags.EmbedLinks,
        PermissionsBitField.Flags.AttachFiles,
        PermissionsBitField.Flags.ManageRoles,
        PermissionsBitField.Flags.ViewChannel,
      ];

      const check = '✅';
      const cross = '❌';

      const permissionNames = Object.fromEntries(
          Object.entries(PermissionsBitField.Flags).map(([key, value]) => [value, key])
      );

      const formatPerms = (perms, checkPermsList) => {
        if (!perms) return 'Permissions data unavailable.';
        return checkPermsList.map(perm => {
          const hasPerm = perms.has(perm);
          const permName = (permissionNames[perm] || `Unknown Flag (${perm})`).replace(/([A-Z])/g, ' $1').trim();
          return `${hasPerm ? check : cross} ${permName}`;
        }).join('\n') || 'No specific permissions checked.';
      };

      const botPermsDisplay = formatPerms(botPermissions, requiredPermissions);
      const channelPermsDisplay = formatPerms(channelPermissions, requiredPermissions);

      const devEmbed = new EmbedBuilder()
          .setColor('#FF0000')
          .setTitle(`🐞 Debug Info: ${targetUser.tag} in ${guild.name}`)
          .setDescription(`Request initiated by: ${interactionUser.tag} (${interactionUser.id})`)
          .addFields(
              { name: 'Server Info', value: `Name: ${guild.name}\nID: ${guild.id}\nMembers: ${guild.memberCount || 'N/A'}`, inline: true },
              { name: 'Target User Info', value: `Tag: ${targetUser.tag}\nID: ${targetUser.id}\nJoined: ${targetMember.joinedTimestamp ? `<t:${Math.floor(targetMember.joinedTimestamp / 1000)}:R>` : 'N/A'}`, inline: true },
              { name: 'Interaction Info', value: `Channel: #${interaction.channel?.name || 'Unknown'} (${interaction.channelId})\nCommand: \`/${interaction.commandName}\``, inline: true },
              { name: 'Bot Permissions (Server)', value: `\`\`\`diff\n${botPermsDisplay}\n\`\`\``, inline: false },
              { name: 'Bot Permissions (Channel)', value: `\`\`\`diff\n${channelPermsDisplay}\n\`\`\``, inline: false },
              { name: '**Guild Configuration**', value: `\`\`\`json\n${JSON.stringify(guildConfig, null, 2)}\n\`\`\``, inline: false },
              { name: '**User Data (from DB)**', value: rawUserData ? `\`\`\`json\n${JSON.stringify(rawUserData, null, 2)}\n\`\`\`` : '`No user data found in database.`', inline: false }
          )
          .setTimestamp()
          .setFooter({ text: `Bot: ${client.user.tag} | Guild ID: ${guild.id}` });


      const userEmbed = new EmbedBuilder()
          .setColor('#00FF00')
          .setTitle('Debug Information Sent')
          .setDescription('Diagnostic information has been forwarded to the developers.')
          .addFields(
              { name: 'Server', value: `${guild.name} (${guild.id})`, inline: true },
              { name: 'Target User', value: `${targetUser.tag} (${targetUser.id})`, inline: true }
          )
          .setTimestamp();


      let debugMessageLink = null;
      try {
        const devGuild = await client.guilds.fetch(developerGuildId).catch(() => null);
        if (!devGuild) {
          console.error(`Debug command: Could not find developer guild ${developerGuildId}`);
          userEmbed.setColor('#FFA500').addFields({ name: 'Delivery Status', value: ':warning: Could not find the developer destination guild.' });
        } else {
          const devChannel = devGuild.channels.cache.get(developerChannelId);
          if (!devChannel || !devChannel.isTextBased()) {
            console.error(`Debug command: Could not find developer channel ${developerChannelId} or it's not text-based.`);
            userEmbed.setColor('#FFA500').addFields({ name: 'Delivery Status', value: ':warning: Could not find the developer destination channel.' });
          } else {
            try {
              const sentDevMessage = await devChannel.send({ embeds: [devEmbed] });
              debugMessageLink = sentDevMessage.url;
            } catch (devSendError) {
              console.error("Failed to send debug information to developer channel:", devSendError);
              userEmbed.setColor('#FF0000').addFields({ name: 'Delivery Status', value: ':x: Failed to send to developers.' });
            }
          }
        }
      } catch (fetchError) {
        console.error("Error fetching developer guild/channel:", fetchError);
        userEmbed.setColor('#FFA500').addFields({ name: 'Delivery Status', value: ':warning: Error accessing developer destination.' });
      }


      if (debugMessageLink) {
        userEmbed.addFields({ name: 'Support Reference Link', value: `[Click Here](${debugMessageLink})` });
        userEmbed.setDescription(userEmbed.data.description + '\nIf support requested this, provide them with the reference link above.');
      }

      await interaction.editReply({ embeds: [userEmbed], ephemeral: true });

    } catch (error) {
      console.error(`Error executing debug command: ${error.message}`, error.stack);
      const errorMsg = 'An unexpected error occurred while generating debug info.';
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({ content: errorMsg, ephemeral: true }).catch(console.error);
      } else {
        await interaction.editReply({ content: errorMsg, embeds: [], components: [] }).catch(console.error);
      }
    }
  }
};
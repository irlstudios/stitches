const fs = require('fs-extra');
const path = require('path');

function getConfigPath(guildId) {
    return path.join(__dirname, '..', 'databases', String(guildId), 'config.json');
}

function getGuildDbPath(guildId) {
    return path.join(__dirname, '..', 'databases', String(guildId));
}


async function getConfig(guildId) {
    const configPath = getConfigPath(guildId);
    try {
        const exists = await fs.pathExists(configPath);
        if (exists) {
            const config = await fs.readJson(configPath);
            ensureConfigStructure(config);
            return config;
        } else {
            console.log(`Config file not found for guild ${guildId} at ${configPath}. Returning null.`);
            return null;
        }
    } catch (error) {
        console.error(`Error reading configuration from ${configPath}:`, error);
        return null;
    }
}

async function saveConfig(guildId, config) {
    const configPath = getConfigPath(guildId);
    const guildDbPath = getGuildDbPath(guildId);

    ensureConfigStructure(config);

    try {
        await fs.ensureDir(guildDbPath);

        await fs.writeJson(configPath, config, { spaces: 2 });

    } catch (error) {
        console.error(`Error saving configuration to ${configPath}:`, error);
        throw error;
    }
}

function ensureConfigStructure(config) {
    if (!config) return;

    if (typeof config.streakSystem !== 'object' || config.streakSystem === null) config.streakSystem = {};
    if (typeof config.messageLeaderSystem !== 'object' || config.messageLeaderSystem === null) config.messageLeaderSystem = {};
    if (typeof config.levelSystem !== 'object' || config.levelSystem === null) config.levelSystem = {};
    if (typeof config.reportSettings !== 'object' || config.reportSettings === null) config.reportSettings = {};

    config.streakSystem = {
        enabled: config.streakSystem.enabled ?? false,
        streakThreshold: config.streakSystem.streakThreshold ?? 10,
        channelStreakOutput: config.streakSystem.channelStreakOutput ?? null,
        isGymClassServer: config.streakSystem.isGymClassServer ?? false,
        ...config.streakSystem
    };
    config.messageLeaderSystem = {
        enabled: config.messageLeaderSystem.enabled ?? false,
        channelMessageLeader: config.messageLeaderSystem.channelMessageLeader ?? null,
        roleMessageLeader: config.messageLeaderSystem.roleMessageLeader ?? null,
        ...config.messageLeaderSystem
    };
    config.levelSystem = {
        enabled: config.levelSystem.enabled ?? false,
        xpPerMessage: config.levelSystem.xpPerMessage ?? 10,
        levelMultiplier: config.levelSystem.levelMultiplier ?? 1.5,
        levelUpMessages: config.levelSystem.levelUpMessages ?? true,
        channelLevelUp: config.levelSystem.channelLevelUp ?? null,
        ...config.levelSystem
    };
    config.reportSettings = {
        weeklyReportChannel: config.reportSettings.weeklyReportChannel ?? null,
        monthlyReportChannel: config.reportSettings.monthlyReportChannel ?? null,
        ...config.reportSettings
    };

    config.lastUpdated = new Date().toISOString();
    if (!Object.prototype.hasOwnProperty.call(config, 'expireAt')) {
        config.expireAt = null;
    }
}

module.exports = { getConfig, saveConfig, ensureConfigStructure };
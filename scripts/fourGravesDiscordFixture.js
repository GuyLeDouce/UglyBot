"use strict";
const {
  Collection,
  PermissionsBitField,
  PermissionFlagsBits: P,
  ChannelType,
} = require("discord.js");
function fixture(guildId) {
  let id = 1000;
  const fail = { role: false, message: false };
  const roles = new Collection(),
    channels = new Collection(),
    members = new Collection();
  const adminRole = {
    id: "900",
    name: "Admin",
    permissions: new PermissionsBitField(P.ManageGuild),
  };
  roles.set(adminRole.id, adminRole);
  function member(userId) {
    const cache = new Collection();
    return {
      id: userId,
      user: { id: userId, username: "user" + userId },
      displayName: "User " + userId,
      roles: {
        cache,
        add: async (role) => {
          if (fail.role) throw Error("Injected role assignment failure");
          cache.set(role.id, role);
        },
        remove: async (role) => cache.delete(role.id),
      },
    };
  }
  for (const user of ["10", "20", "999"]) members.set(user, member(user));
  const guild = {
    id: guildId,
    members: {
      fetch: async () => members,
      fetchMe: async () => ({
        permissions: new PermissionsBitField([P.ManageRoles, P.ManageChannels]),
      }),
    },
    roles: {
      cache: roles,
      fetch: async () => roles,
      create: async (args) => {
        const role = {
          id: String(++id),
          ...args,
          permissions: new PermissionsBitField(args.permissions),
        };
        roles.set(role.id, role);
        return role;
      },
    },
    channels: {
      cache: channels,
      fetch: async (channelId) =>
        channelId ? channels.get(channelId) : channels,
      create: async (args) => {
        const messages = new Collection();
        const channel = {
          id: String(++id),
          guildId,
          type: ChannelType.GuildText,
          ...args,
          overwrites: args.permissionOverwrites,
          permissionOverwrites: {
            set: async (ow) => {
              channel.overwrites = ow;
            },
          },
          messages: {
            fetch: async (input) => {
              if (typeof input === "string") {
                if (!messages.has(input))
                  throw Object.assign(Error("Missing message"), {
                    code: 10008,
                  });
                return messages.get(input);
              }
              return messages;
            },
          },
          send: async (payload) => {
            if (fail.message) throw Error("Injected message send failure");
            const m = {
              id: String(++id),
              author: { id: "bot" },
              embeds: payload.embeds,
              components: payload.components.map((c) => ({ toJSON: () => c })),
              edit: async (p) => {
                if (fail.message) throw Error("Injected message edit failure");
                m.embeds = p.embeds;
                m.components = p.components.map((c) => ({ toJSON: () => c }));
              },
            };
            messages.set(m.id, m);
            return m;
          },
        };
        channels.set(channel.id, channel);
        return channel;
      },
    },
  };
  return {
    guild,
    members,
    roles,
    channels,
    fail,
    deps: {
      client: { user: { id: "bot" }, guilds: { fetch: async () => guild } },
      enabled: () => true,
      adminIds: () => new Set(["admin"]),
    },
  };
}
module.exports = { fixture };

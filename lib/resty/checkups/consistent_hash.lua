-- Copyright (C) 2014-2016, UPYUN Inc.

local resty_chash = require 'resty.chash'

local _M = { _VERSION = "0.11" }

local function init_consistent_hash_state(servers)
    local data, nodes = table.new(0, 1024), table.new(0, 1024)
    local members = 0
    local str_null = string.char(0)
    for index, srv in ipairs(servers) do
        local hash_id = ("%s%s%s"):format(srv.host, str_null, srv.port)
        data[hash_id] = index
        nodes[hash_id] = srv.weight
        members = members + 1
    end
    local chash = resty_chash:new(nodes)
    return { circle = chash, members = members, data = data }
end

function _M.next_consistent_hash_server(servers, peer_cb, hash_key)
    local is_tab = require "resty.checkups.base".is_tab
    servers.chash = is_tab(servers.chash) and servers.chash
                    or init_consistent_hash_state(servers)

    local chash = servers.chash
    if chash.members == 1 then
        if peer_cb(1, servers[1]) then
            return servers[1]
        end

        return nil, "consistent hash: no servers available"
    end

    local circle = chash.circle
    local data = chash.data
    local hash_id, hash_idx = circle:find(hash_key)
    
    for i = 1, circle.size do
        local id = data[hash_id]
        if peer_cb(id, servers[id]) then
            return servers[id]
        end
        hash_id, hash_idx = circle:next(hash_idx)
    end

    return nil, "consistent hash: no servers available"
end


function _M.free_consitent_hash_server(srv, failed)
    return
end


return _M

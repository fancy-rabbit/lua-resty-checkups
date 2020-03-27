-- Copyright (C) 2014-2016, UPYUN Inc.

local floor      = math.floor
local tab_sort   = table.sort
local tab_insert = table.insert

local _M = { _VERSION = "0.11" }

local REPLICAS  = 160


local function hash_string(str)
    return ngx.crc32_short(str)
end


local function init_consistent_hash_state(servers)
    local circle, members = {}, 0
    for index, srv in ipairs(servers) do
        local key = ("%s%s%s"):format(srv.host, string.char(0), srv.port)
        local base_hash = hash_string(key)
        local prev_hash = 0
        for c = 1, REPLICAS * (srv.weight or 1) do
            key = ("%s%s"):format(base_hash, prev_hash)
            local hash = hash_string(key)
            tab_insert(circle, { hash, index })
            prev_hash = hash
        end
        members = members + 1
    end

    tab_sort(circle, function(a, b) return a[1] < b[1] end)

    return { circle = circle, members = members }
end


local function binary_search(circle, key)
    local size = #circle
    local st, ed, mid = 1, size
    while st <= ed do
        mid = floor((st + ed) / 2)
        if circle[mid][1] < key then
            st = mid + 1
        else
            ed = mid - 1
        end
    end

    return st == size + 1 and 1 or st
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
    local st = binary_search(circle, hash_string(hash_key))
    local size = #circle
    local ed = st + size - 1
    for i = st, ed do  -- TODO: algorithm O(n)
        local idx = circle[(i - 1) % size + 1][2]
        if peer_cb(idx, servers[idx]) then
            return servers[idx]
        end
    end

    return nil, "consistent hash: no servers available"
end


function _M.free_consitent_hash_server(srv, failed)
    return
end


return _M

local M = {}

local ngx = require "ngx"
local cjson = require "cjson"
local Flexihash = require 'Flexihash'

-- Create a consistent hash of the values given in a table
function M.create_hash_map(values)
    local hash_map = Flexihash.New()
    for _,value in pairs(values) do
        ngx.log(ngx.DEBUG,"Adding value " .. value .. " of type " .. type(value) .. " to the hash")
        hash_map:addTarget(value)
    end
    return hash_map
end

-- Return a table with the sites where a given object fits according to the
-- hash ring.
function M.look_up_hash_map(hash, hash_map, replicas)
    local result = hash_map:lookupList(hash, replicas)
    return result
end

-- Return a table with the hosts at a specific site  where a given object fits
-- according to the hash ring.
function M.get_object_replica_site_hosts(bucket, object, site)
    -- TODO: Should store the hash map in memory to avoid having to create it
    -- for each request.
    local hosts = M.get_site_hosts(site)
    local hash_map = M.create_hash_map(hosts)

    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which hosts to use for this object.
    local hash = bucket .. object
    local replicas = M.get_replicas_per_site()
    local result = M.look_up_hash_map(hash, hash_map, replicas)
    return result
end

-- Return a table with the sites where a given object fits according to the
-- hash ring.
function M.get_object_replica_sites(bucket, object)
    -- TODO: Should store the hash map in memory to avoid having to create it
    -- for each request.
    local sites = M.get_sites()
    local hash_map = M.create_hash_map(sites)

    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which sites to use for this object.
    local hash = bucket .. object
    local replicas = M.get_replica_sites()
    local result = M.look_up_hash_map(hash, hash_map, replicas)
    return result
end

-- Return a table with the hosts and sites where a given object fits
-- according to the hash ring.
function M.get_object_replica_hosts(bucket, object, sites)
    local hosts = {}
    for _,site in pairs(sites) do
        local result = M.get_replica_site_hosts(bucket, object, site)
        for _, host in pairs(result) do
            table.insert(hosts, host)
        end
    end
    return hosts
end

return M

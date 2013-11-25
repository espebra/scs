local M = {}

local ngx = require "ngx"
local cjson = require "cjson"
local Flexihash = require 'Flexihash'
local common = require 'common'

-- Hash lookup
-- Input hash, haystack, count
-- Return table with the hits
function M.hash_lookup(hash, haystack, count)
    -- Create the hash map.
    -- TODO: Should store the hash map in memory to avoid having to create it
    -- for each request.
    local hash_map = Flexihash.New()
    for _,value in pairs(haystack) do
        ngx.log(ngx.DEBUG,"Adding value " .. value .. " of type " .. type(value) .. " to the hash")
        hash_map:addTarget(value)
    end

    -- Lookup the hash in the hash map, return a table with the hits
    local result = hash_map:lookupList(hash, count)
    return result
end

-- Return a table with the hosts and sites where a given object fits
-- according to the hash ring.
function M.get_object_replica_hosts(bucket, object, replica_sites, replicas_per_site)
    local h = {}
    local hash = bucket .. object

    local all_sites = common.get_sites()
    local sites = M.hash_lookup(hash, all_sites, replica_sites)
    for _,site in pairs(sites) do
        local site_hosts = common.get_hosts(site)
        local result = M.hash_lookup(hash, site_hosts, replicas_per_site)
        for _, host in pairs(result) do
            table.insert(h, host)
        end
    end
    return h
end

return M

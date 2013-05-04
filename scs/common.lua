local M = {}

local cjson = require "cjson"
local Flexihash = require 'Flexihash'
local http = require "resty.http.simple"
local ngx = require "ngx"

-- Read a json file
local function read_json_file(path, required)
    local f = nil
    local conf = {}
    if required then
        f = assert(io.open(path, "r"))
    else
        f = io.open(path, "r")
    end
    if f then
        local c = f:read("*all")
        f:close()
        conf = cjson.decode(c)
    end
    return conf
end

-- Check if an object exists in the local file system
function M.object_exists_locally(dir, bucket, object_base64)
   local path = dir .. "/" ..  bucket .. "/" .. object_base64
   return M.is_file(path)
end

-- Return the value of a given request header, or nil if the header is not set
function M.get_header(header,headers)
    if headers[header] then
        return headers[header]
    end
    return nil
end

-- Check if a path is a file in the local file system
function M.is_file(path)
   local f=io.open(path,"r")
   if f~=nil then
       io.close(f)
       return true
    else
        return false
    end
end

-- Check if an object exists on a remote host
function M.object_exists_on_remote_host(internal,host,port,bucket,object)
    headers = {}
    headers['x-bucket'] = bucket
    headers['user-agent'] = "scs internal"

    local res, err = http.request(host, port, {
        method  = "HEAD",
        version = 0,
        path    = "/" .. object,
        timeout = 500,
        headers = headers
    })
    if not res then
        -- ngx.say("http failure: ", err)
        return nil
    end
    if res.status >= 200 and res.status < 300 then
        return host
    else
        return false
    end
end

function M.remote_host_availability(host, port)
    headers = {}
    headers['x-status'] = true
    headers['user-agent'] = "scs internal"

    local res, err = http.request(host, port, {
        method  = "HEAD",
        version = 0,
        path    = "/",
        timeout = 500,
        headers = headers
    })
    if not res then
        return nil
    end
    if res.status >= 200 and res.status < 300 then
        return true
    else
        return false
    end
end

-- Create a consistent hash of the values given in a table
function M.create_hash_map(values)
    local hash_map = Flexihash.New()
    local i = ""
    for _,value in pairs(values) do
        hash_map:addTarget(value)
        i = i .. " " .. value
    end
    return hash_map
end

function M.generate_url(host, port, object)
    local url
    if port == 80 then
        url = "http://" .. host .. "/" .. object
    else
        url = "http://" .. host .. ":" .. port .. "/" .. object
    end
    return url
end

function M.get_configuration()
    local path = "/etc/scs/scs.json"
    local config = read_json_file(path, true)
    return config
end

-- Return a table containing the sites in the configuration
function M.get_all_sites(config)
    local sites = {}
    for site,_ in pairs(config.current.hosts) do
        table.insert(sites,site)
    end
    return sites
end

-- Return a table with the sites where a given object fits according to the
-- hash ring.
function M.look_up_hash_map(hash, hash_map, replicas)
    local result = hash_map:lookupList(hash, replicas)
    return result
end

function M.sync_object(dir, host, bucket, object_base64)
    --local cmd="/usr/bin/rsync -zSut " .. dir .. "/" .. bucket .. "/" .. object_base64 .. " rsync://" .. host .. "/scs/" .. bucket .. "/" .. object_base64
    local cmd="/usr/bin/rsync -zSut " .. dir .. "/" .. bucket .. "/" .. object_base64 .. " rsync://" .. host .. "/scs/" .. bucket .. "/"
    local res = os.execute(cmd)
    if res == 0 then
        return true
    else
        return false
    end
end

return M


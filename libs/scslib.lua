module(..., package.seeall)
local Flexihash = require 'libs.Flexihash'

local function read_configuration_file(path, required)
    -- Read the current configuration
    local cjson = require "cjson"
    local f
    io.open(path, "r")
    if required then
        f = assert(io.open(path, "r"))
    else
        f = io.open(path, "r")
    end
    local c = f:read("*all")
    f:close()
    local conf = cjson.decode(c)
    return conf
end

-- Check if an object exists in the local file system
function object_exists_locally(dir, bucket, object_base64)
   local path = dir .. "/" ..  bucket .. "/" .. object_base64
   return is_file(path)
end

-- Return the value of a given request header, or nil if the header is not set
function get_header(header,headers)
    if headers[header] then
        return headers[header]
    end
    return nil
end

-- Check if a path is a file in the local file system
function is_file(path)
   local f=io.open(path,"r")
   if f~=nil then
       io.close(f)
       return true
    else
        return false
    end
end

-- Check if an object exists on a remote host
function object_exists_on_remote_host(internal,host,port,bucket,object)
    local http = require "resty.http.simple"
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
        return true
    else
        return false
    end
end

function remote_host_availability(host, port)
    local http = require "resty.http.simple"
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
function create_hash_map(values)
    local hash_map = Flexihash.New()
    local i = ""
    for _,value in pairs(values) do
        hash_map:addTarget(value)
        i = i .. " " .. value
    end
    return hash_map
end

function generate_url(host, port, object)
    local url
    if port == 80 then
        url = "http://" .. host .. "/" .. object
    else
        url = "http://" .. host .. ":" .. port .. "/" .. object
    end
    return url
end

function get_configuration()
    local path = "/etc/scs/scs.json"
    local config = read_configuration_file(path, true)
    return config
end

-- Return a table containing the sites in the configuration
function get_all_sites(config)
    local sites = {}
    for site,_ in pairs(config.current.hosts) do
        table.insert(sites,site)
    end
    return sites
end


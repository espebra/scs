local M = {}

local cjson = require "cjson"
local Flexihash = require 'Flexihash'
local http = require "resty.http.simple"
local ngx = require "ngx"

-- Read a json file
local function read_file(path, required)
    local f = nil
    local content = false
    if required then
        f = assert(io.open(path, "r"))
    else
        f = io.open(path, "r")
    end
    if f then
        content = f:read("*all")
        f:close()
    end
    return content
end

-- Return the host availability
function M.get_host_status(host)
    local s = ngx.shared.status
    local value, flags = s:get(host)

    -- Return only true / false
    if value == nil then
        value = false
    end

    -- if value then
    --     ngx.log(ngx.ERR,"Host " .. host .. " is up")
    -- else
    --     ngx.log(ngx.ERR,"Host " .. host .. " is down")
    -- end
    return value
end

-- Update the status for a host
function M.update_host_status(host,status)
    local s = ngx.shared.status
    local success, err, forcible
    if status then
        success, err, forcible = s:set(host, true)
    else
        success, err, forcible = s:set(host, false)
    end
    if success then
        return true
    else
        ngx.log(ngx.ERR,"Failed to cache status for host " .. host .. ": " .. err)
        return false
    end
end

-- Verify that the bucket name is valid
function M.verify_bucket(bucket)
    if not bucket then
        return false
    end
    if #bucket < 3 then
        return false
    end
    if #bucket > 40 then
        return false
    end
    if not ngx.re.match(bucket, '^[a-zA-Z0-9]+$','j') then
        return false
    end
    return true
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

function M.http_request(host, port, headers, method, path)
    local res, err = http.request(host, port, {
        method  = method,
        version = 0,
        path    = path,
        timeout = 1000,
        headers = headers
    })
    if not res then
        ngx.log(ngx.ERR,"Unable to execute " .. method .. " to http://" .. host .. ":" .. port .. path .. ": " .. err)
        return nil
    end
    if res.status >= 200 and res.status < 300 then
        return true
    else
        return false
    end

    -- local client = http:new()
    -- client:set_timeout(1000)

    -- local ok, err = client:connect(host, port)
    -- if not ok then
    --     ngx.log(ngx.ERR,"Failed to connect to " .. host .. ":" .. port .. ": " .. err)
    --     return nil
    -- end

    -- local res, err = client:request({
    --     method  = method,
    --     path    = path,
    --     headers = headers
    -- })

    -- if not res then
    --     ngx.log(ngx.ERR,"Failed to retrieve " .. host .. path .. ": " .. err)
    --     return nil
    -- end

    -- -- close connection, or put it into the connection pool
    -- if res.headers["connection"] == "close" then
    --     local ok, err = client:close()
    --     if not ok then
    --         ngx.log(ngx.ERR,"Failed to close: ", err)
    --         return nil
    --     end
    -- else
    --     client:set_keepalive(0, 100)
    -- end

    -- if res.status >= 200 and res.status < 300 then
    --     return true
    -- else
    --     return false
    -- end
end

-- Check if an object exists on a remote host
function M.object_exists_on_remote_host(host, port, bucket, object)
    local method = "HEAD"
    local path = "/" .. object
    local headers = {}
    headers['x-bucket'] = bucket
    headers['user-agent'] = "scs internal"

    local res = M.http_request(host, port, headers, method, path)
    if res then
        return host
    else
        return res
    end
end

function M.remote_host_availability(host, port)
    local method = "HEAD"
    local path = "/"
    local headers = {}
    headers['x-status'] = true
    headers['user-agent'] = "scs internal"

    return M.http_request(host, port, headers, method, path)
end

-- Create a consistent hash of the values given in a table
function M.create_hash_map(values)
    local hash_map = Flexihash.New()
    for _,value in pairs(values) do
        -- ngx.log(ngx.ERR,"Adding value " .. value .. " of type " .. type(value) .. " to the hash")
        hash_map:addTarget(value)
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

function M.get_write_back()
    local conf = M.get_configuration()
    return conf.current.write_back
end

function M.get_replicas_per_site()
    local conf = M.get_configuration()
    return conf.current.replicas_per_site
end

function M.get_replica_sites()
    local conf = M.get_configuration()
    return conf.current.replica_sites
end

function M.get_bind_port()
    local conf = M.get_configuration()
    return conf.current.bind_port
end

function M.get_storage_directory()
    local conf = M.get_configuration()
    return conf.current.storage_directory
end

function M.get_configuration()
    local c = ngx.shared.conf
    local json = c:get('conf')
    -- local conf, flags = c:get('conf')
    if not json then
        local path = "/etc/scs/scs.json"
        json = read_file(path, true)
        c:set('conf', json)
        ngx.log(ngx.ERR, "Caching: Configuration")
    end
    local conf = cjson.decode(json)
    return conf
end

-- Return a table containing the hosts in a given site
function M.get_site_hosts(site)
    local hosts = {}
    local conf = M.get_configuration()
    for host,h in pairs(conf.current.hosts) do
        if h['site'] == site then
            table.insert(hosts,host)
            -- ngx.log(ngx.ERR,"Caching: Host " .. host .. " is in site " ..  h['site'])
        end
    end
    
    -- ngx.log(ngx.ERR,"Returned " .. #hosts .. " hosts in site " .. site)
    return hosts
end

-- Return a table containing the sites in the configuration
function M.get_all_sites()
    local sites = ngx.shared.sites
    if not sites then
        sites = {}
        local conf = M.get_configuration()
        for host,h in pairs(conf.current.hosts) do
            if not M.inTable(sites, h['site']) then
                table.insert(sites,h['site'])
                ngx.log(ngx.ERR,"Caching: Site " .. h['site'] .. " is one of our sites")
            end
        end
        ngx.shared.sites = sites
    end
    return sites
end

function M.inTable(tbl, item)
    for key, value in pairs(tbl) do
        if value == item then return key end
    end
    return false
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

-- Return a table with the hosts at a specific site  where a given object fits
-- according to the hash ring.
function M.get_replica_site_hosts(bucket, object, site)
    -- Try to read the hash map from shared memory
    --local hash_map = ngx.shared[site]
    local hash_map = false
    if not hash_map then
        -- If the hash map does not exist, create it and store it for later use
        local hosts = M.get_site_hosts(site)
        hash_map = M.create_hash_map(hosts)
        --ngx.shared[site] = hash_map
    end
    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which hosts to use for this object.
    local hash = bucket .. object
    local replicas = M.get_replicas_per_site()
    local result = M.look_up_hash_map(hash, hash_map, replicas)
    return result
end

-- Return a table with the hosts and sites where a given object fits
-- according to the hash ring.
function M.get_replica_hosts(bucket, object, sites)
    local hosts = {}
    for _,site in pairs(sites) do
        local result = M.get_replica_site_hosts(bucket, object, site)
        for _, host in pairs(result) do
            table.insert(hosts, host)
        end
    end
    return hosts
end

-- Return a table with the sites where a given object fits according to the
-- hash ring.
function M.get_object_replica_sites(bucket, object)
    -- Try to read the hash map from shared memory
    -- local hash_map = ngx.shared.sites_hash_map
    local hash_map = false
    if not hash_map then
        -- If the hash map does not exist, create it and store it for later use
        local sites = M.get_all_sites()
        hash_map = M.create_hash_map(sites)
        -- ngx.shared.sites_hash_map = hash_map
    end
    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which sites to use for this object.
    local hash = bucket .. object
    local replicas = M.get_replica_sites()
    local result = M.look_up_hash_map(hash, hash_map, replicas)
    return result
end

-- Function to randomize a table
function M.randomize_table(t)
    for i = #t, 2, -1 do
        -- select a random number between 1 and i
        local r = math.random(i)
         -- swap the randomly selected item to position i
        t[i], t[r] = t[r], t[i]
    end
    return t
end

-- Figure out exactly which host to use from the hosts given from the hash
-- ring lookup,
function M.get_host_with_object(hosts, bucket, object)
    -- Randomize the hosts table
    -- backwards
    hosts = M.randomize_table(hosts)

    -- Return nil if no hosts are up
    local status = nil

    local port = M.get_bind_port()

    -- For each host, check if the object is available. Return the first
    -- host that has the object available.
    local threads = {}
    for i,host in pairs(hosts) do
        -- Only test hosts that are up
        if M.get_host_status(host) then
            --ngx.log(ngx.ERR,"Checking host " .. host)
            -- At least one host is up. Let's change the exit status to false
            status = false
            table.insert(threads,ngx.thread.spawn(M.object_exists_on_remote_host, host, port, bucket, object))
        end
    end

    for i = 1, #threads do
        local ok, res = ngx.thread.wait(threads[i])
        if not ok then
            ngx.log(ngx.ERR,"Thread " .. i .. " failed to run")
        else
            if res then
                return res
            end
        end
    end
    return status
end

-- function M.get_available_replica_hosts(hosts)
--     -- Randomize the hosts table
--     -- backwards
--     for i = #hosts, 2, -1 do
--         -- select a random number between 1 and i
--         local r = math.random(i)
--          -- swap the randomly selected item to position i
--         hosts[i], hosts[r] = hosts[r], hosts[i]
--     end
-- 
--     -- For each host, check if the object is available. Return the first
--     -- host that has the object available.
--     local port = M.get_bind_port()
--     local available_hosts = {}
--     local threads = {}
--     for i,host in pairs(hosts) do
--         if M.get_host_status(host) then
--             table.insert(threads,ngx.thread.spawn(M.remote_host_availability, host, port))
--         end
--     end
-- 
--     for i = 1, #threads do
--         local ok, res = ngx.thread.wait(threads[i])
--         if ok then
--             if res then
--                 table.insert(available_hosts, hosts[i])
--             end
--         end
--     end
-- 
--     -- If any of the hosts are available, return nil
--     return available_hosts
-- end

function M.object_fits_on_this_host(hosts)
    for _,host in pairs(hosts) do
        if ngx.req.get_headers()["Host"] == host then
            return true
        end
    end
    return false
end

function M.is_internal_request(useragent)
    if useragent then
        if useragent == "scs internal" then
            return true
        end
    end
    return false
end

function M.parse_request()
    local h = ngx.req.get_headers()
    local internal = M.is_internal_request(h['user-agent'])
    local debug = h['x-debug']
    local status = h['x-status']
    local bucket = h['x-bucket']

    -- Read the object name, and remove the first char (which is a /)
    local object = string.sub(ngx.var.request_uri, 2)
    -- Unescape the filename of the object before hashing
    object = ngx.unescape_uri(object)

    if not M.verify_bucket(bucket) then
        bucket = false
    end

    local r = {
        -- Plain text name of the object
        ['object'] = object,
        -- Base64 name of the object
        ['object_base64'] = ngx.encode_base64(object), 
        -- Request method (HEAD, GET, POST, ..)
        ['method'] = ngx.var.request_method, 
        -- Name of the bucket
        ['bucket'] = bucket, 
        -- True if internal signaling request
        ['internal'] = internal, 
        -- Add debug information in the response
        ['status'] = status, 
        -- Add debug information in the response
        ['debug'] = debug, 
    }
    return r
end

return M

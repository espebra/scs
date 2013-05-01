local scs = require "libs.scslib"
local http = require "libs.resty.http.simple"
local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

-- Verify that the bucket name is valid
local function verify_bucket(bucket)
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

-- Return a table with the sites where a given object fits according to the
-- hash ring.
local function get_sites(bucket, object)
    -- Try to read the hash map from shared memory
    local site_hash_map = ngx.shared.sites
    if not site_hash_map then
        -- If the hash map does not exist, create it and store it for later use
        local sites = scs.get_all_sites(config)
        site_hash_map = scs.create_hash_map(sites)
        ngx.shared.sites = site_hash_map
    end
    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which sites to use for this object.
    local h = bucket .. object
    local result = site_hash_map:lookupList(h, config.current.replica_sites)
    return result
end

-- Return a table with the hosts at a specific site  where a given object fits 
-- according to the hash ring.
local function get_site_hosts(site, bucket, object) 
    -- Try to read the hash map from shared memory
    local map = ngx.shared[site]
    if not map then
        -- If the hash map does not exist, create it and store it for later use
        map = scs.create_hash_map(config.current.hosts[site])
        ngx.shared[site] = map
    end
    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which hosts to use for this object.
    local h = bucket .. object
    local result = map:lookupList(h, config.current.replicas_per_site)
    return result
end

-- Return a table with the hosts and sites where a given object fits 
-- according to the hash ring.
local function get_hosts(bucket, object)
    local h = bucket .. object
    local sites = get_sites(bucket, object)
    local hosts = {}
    for _,site in pairs(sites) do 
        local result = get_site_hosts(site, bucket, object)
        for _, host in pairs(result) do
            table.insert(hosts, host)
        end
    end 
    return hosts
end

-- Figure out exactly which host to use from the hosts given from the hash
-- ring lookup,
local function get_host_with_object(hosts, bucket, object)
    -- Randomize the hosts table
    -- backwards
    for i = #hosts, 2, -1 do
        -- select a random number between 1 and i
        local r = math.random(i)
         -- swap the randomly selected item to position i
        hosts[i], hosts[r] = hosts[r], hosts[i]
    end 

    -- For each host, check if the object is available. Return the first
    -- host that has the object available.
    for _,host in pairs(hosts) do
        local port = config.current.bind_port
        status = scs.object_exists_on_remote_host(true,host,port,bucket,object)
        if status then
            return host
        end
    end

    -- If the object is not available on any of the hosts, return nil
    return nil
end

local function get_available_host(hosts)
    -- Randomize the hosts table
    -- backwards
    for i = #hosts, 2, -1 do
        -- select a random number between 1 and i
        local r = math.random(i)
         -- swap the randomly selected item to position i
        hosts[i], hosts[r] = hosts[r], hosts[i]
    end 

    -- For each host, check if the object is available. Return the first
    -- host that has the object available.
    local port = config.current.bind_port
    for _,host in pairs(hosts) do
        status = scs.remote_host_availability(host, port)
        if status then
            return host
        end
    end

    -- If any of the hosts are available, return nil
    return nil
end

-- Read the configuration
local function get_cached_configuration()
    -- Try to read the configuration from the shared memory
    local conf = ngx.shared.conf
    if not conf then
        conf = scs.get_configuration()
        ngx.shared.conf = conf
        ngx.log(ngx.ERR, "Caching configuration")
    end

    return conf
end

local function object_fits_on_this_host(hosts)
    for _,host in pairs(hosts) do
        if ngx.req.get_headers()["Host"] == host then
            return true
        end
    end
    return false
end

local function head_object(internal, bucket, object)
    local exitcode = 404
    local msg = nil

    -- See if the object exists locally
    local object_base64 = ngx.encode_base64(object)
    local dir = config.current.storage_directory
    if scs.object_exists_locally(dir, bucket, object_base64) then
        exitcode = 200
        msg = "The object " .. object .. " in bucket " .. bucket .. " exists locally."
    end

    -- The object do not exist locally
    if not internal then
        -- Redirect to another host if this is not an internal request
        local hosts = get_hosts(bucket, object)
        local host = get_host_with_object(hosts, bucket, object)

        -- Easier to understand what is happening when debugging
        local hosts_text = "["
        for _,host in pairs(hosts) do
            hosts_text = hosts_text .. " " .. host 
        end
        hosts_text = hosts_text .. " ]"
    
        if host == nil then

            msg = "The object " .. object .. " in bucket " .. bucket .. " does not exist locally or on any of the available replica hosts " .. hosts_text
            exitcode = 404
        else
            local port = config.current.bind_port
            local url = scs.generate_url(host,port,object)
            msg = 'Redirecting HEAD request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
            ngx.header["Location"] = url
            exitcode = 302
        end
    end
    return exitcode, msg
end

local function get_object(internal, bucket, object)
    local exitcode = 404
    local msg = nil
    -- See if the object exists locally
    local object_base64 = ngx.encode_base64(object)
    local dir = config.current.storage_directory
    if scs.object_exists_locally(dir, bucket, object_base64) then
        -- We have the file locally. Serve it directly. 200.
        ngx.header["content-disposition"] = "attachment; filename=" .. object;
        local path = config.current.storage_directory .. "/" ..  bucket
        local fp = io.open(path .. "/" .. object_base64, 'r')
        local size = 2^13      -- good buffer size (8K)
        -- Stream the contents of the file to the client
        while true do
            local block = fp:read(size)
            if not block then break end
            ngx.print(block)
        end
        fp:close()
        local msg = "Object " .. object .. " in bucket " .. bucket .. " delivered successfully to the client."
        exitcode = 200
    else
        -- The object do not exist locally
        if not internal then
            -- We do not have the file locally. Should lookup the hash table to find a
            -- valid host to redirect to. 302.
            local hosts = get_hosts(bucket, object)
            local host = get_host_with_object(hosts, bucket, object)

            -- Easier to understand what is happening when debugging
            local hosts_text = "["
            for _,host in pairs(hosts) do
                hosts_text = hosts_text .. " " .. host 
            end
            hosts_text = hosts_text .. " ]"
        
            if host == nil then
                msg = "The object " .. object .. " in bucket " .. bucket .. " was not found on any of the available replica hosts " .. hosts_text
            else
                local port = config.current.bind_port
                local url = scs.generate_url(host,port,object)
                -- ngx.say("Host: " .. host)
                -- ngx.say("Redirect to: " .. url)
                msg = 'Redirecting GET request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
                ngx.header["Location"] = url
                exitcode = 302
            end
        end
    end
    return exitcode, msg
end

local function post_object(internal, bucket, object)
    local hosts = get_hosts(bucket,object)
    local exitcode = 404
    local msg = nil

    if object_fits_on_this_host(hosts) then
        local object_base64 = ngx.encode_base64(object)
        local path = config.current.storage_directory .. "/" ..  bucket
        if not os.rename(path, path) then
            os.execute('mkdir -p ' .. path)
        end

        ngx.req.read_body()
        local req_body_file = ngx.req.get_body_file()

        if not req_body_file then
            msg = 'No file found in request'
            exitcode = 400
        end

        if req_body_file == nil then
            msg = 'Request body is nil'
            exitcode = 400
        end

        tmpfile = io.open(req_body_file)
        realfile = io.open(path .. "/" .. object_base64, 'w')

        local size = 2^13      -- good buffer size (8K)
        while true do
            local block = tmpfile:read(size)
            if not block then 
                break
            end
            realfile:write(block)
        end
        tmpfile:close()
        realfile:close()

        local storage_directory = config.current.storage_directory
        if scs.object_exists_locally(storage_directory, bucket, object_base64) then
            msg = 'The object ' .. object .. ' in bucket ' .. bucket .. ' was written successfully to local file system.'
            exitcode = 200
        else
            msg = 'Failed to write object ' .. object .. ' in bucket ' .. bucket .. ' to local file system'
            exitcode = 503
        end
    else
        local host = get_available_host(hosts)

        -- Easier to understand what is happening when debugging
        local hosts_text = "["
        for _,host in pairs(hosts) do
            hosts_text = hosts_text .. " " .. host 
        end
        hosts_text = hosts_text .. " ]"
        
        if host == nil then
            msg = 'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment ' .. hosts_text
            exitcode = 503
        else
            -- Redirect to one of the corrent hosts here. 307.
            local port = config.current.bind_port
            local url = scs.generate_url(host,port,object)
            ngx.header["Location"] = url
            msg = 'Redirecting POST request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
            exitcode = 307
        end
    end
    return exitcode, msg
end

local function put_object(internal, bucket, object, req_body_file)
    local msg
    local exitcode=200
    ngx.req.read_body()
    local req_body_file = ngx.req.get_body_file()
    return exitcode, msg
end

local function delete_object(internal, bucket, object)
    local msg
    local exitcode=200
    return exitcode, msg
end

local function is_internal_request(useragent)
    if useragent then
        if useragent == "scs internal" then
            return true
        end
    end
    return false
end

local internal = is_internal_request(ngx.req.get_headers()['user-agent'])
local debug = ngx.req.get_headers()['x-debug']
local status = ngx.req.get_headers()['x-status']
local bucket = ngx.req.get_headers()['x-bucket']

local exitcode = nil
local msg = nil

-- Return 200 immediately if the x-status header is set. This is to verify that
-- the host is up and running.
if status and internal then
    exitcode = 200
    msg = "Returning 200 to the status check."
end

if not status and not bucket then
    exitcode = 400
    msg = "The request is missing the x-bucket header."
end

if not status and not verify_bucket(bucket) then
    exitcode = 400
    if bucket == nil then
        msg = "Invalid bucket name."
    else
        msg = "Invalid bucket name (" .. bucket .. ")."
    end
end

-- Read the object name, and remove the first char (which is a /)
local object = string.sub(ngx.var.request_uri, 2)
if not status and string.len(object) == 0 then
    exitcode = 400
    msg = "The object name is not set."
end

-- If the preflight checks went OK, go on with the real work here
config = get_cached_configuration()

if not exitcode then
    local method = ngx.var.request_method
    if method == 'HEAD' then
        exitcode, msg = head_object(internal, bucket, object)
    elseif method == "GET" then
        exitcode, msg = get_object(internal, bucket, object)
    elseif method == "POST" then
        exitcode, msg = post_object(internal, bucket, object)
    elseif method == "PUT" then
        exitcode, msg = put_object(internal, bucket, object)
    elseif method == "DELETE" then
        exitcode, msg = delete_object(internal, bucket, object)
    end
end

local elapsed = ngx.now() - ngx.req.start_time()
-- ngx.header["x-elapsed"] = elapsed

if debug then
    if msg then
        ngx.log(ngx.ERR, "Req time: " .. elapsed .. " sec. " .. msg)
    else
        ngx.log(ngx.ERR, "Req time: " .. elapsed .. " sec. No message")
    end
end
ngx.exit(exitcode)

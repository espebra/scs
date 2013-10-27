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
function M.update_host_status(host, status)
    local s = ngx.shared.status

    local previous_status = s:get(host)
    if not previous_status == status then
        if status then
            ngx.log(ngx.ERR,"Host " .. host .. " is now up!")
        else
            ngx.log(ngx.ERR,"Host " .. host .. " is now unavailable!")
        end
    end

    local success, err, forcible
    success, err, forcible = s:set(host, true)

    if success then
        return true
    else
        ngx.log(ngx.ERR,"Failed to cache status for host " .. host .. ": " .. err)
        return false
    end
end

-- Verify that the bucket name is valid
function M.verify_bucket(bucket)

    -- Must not be false
    if not bucket then
        --ngx.log(ngx.WARN,"Bucket name is not set")
        return false
    end

    -- Must not be less than 3 characters
    if #bucket < 3 then
        --ngx.log(ngx.WARN,"Bucket name is too short")
        return false
    end

    -- Must not be more than 63 characters
    if #bucket > 63 then
        --ngx.log(ngx.WARN,"Bucket name is too long")
        return false
    end

    -- Must contain only allowed characters
    if not ngx.re.match(bucket, '^[a-z0-9-]+$','j') then
        --ngx.log(ngx.WARN,"Bucket name contains illegal characters")
        return false
    end

    -- Must not start with -
    if ngx.re.match(bucket, '^-','j') then
        --ngx.log(ngx.WARN,"Bucket name starts with -")
        return false
    end

    -- Must not end with -
    if ngx.re.match(bucket, '-$','j') then
        --ngx.log(ngx.WARN,"Bucket name ends with -")
        return false
    end

    return true
end

-- Check if an object exists in the local file system
-- function M.object_exists_locally(dir, bucket, object_base64)
--    local path = dir .. "/" ..  bucket .. "/" .. object_base64
--    return M.is_file(path)
-- end

-- Return the value of a given request header, or nil if the header is not set
function M.get_header(header, headers)
    if headers[header] then
        return headers[header]
    end
    return nil
end

-- Return the value of a given request parameter, or nil if the parameter is not set
function M.get_parameter(parameter, parameters)
    if parameters[parameter] then
        return parameters[parameter]
    end
    return nil
end

-- Check if a path is a file in the local file system
function M.is_file(path)
   if path then
       local f=io.open(path,"r")
       if f~=nil then
           io.close(f)
           return true
       end
   end
   return false
end

function M.http_request(host, port, headers, method, path, timeout)
    local res, err = http.request(host, port, {
        method  = method,
        version = 0,
        path    = path,
        timeout = timeout,
        headers = headers
    })
    if not res then
        ngx.log(ngx.DEBUG,"Unable to execute " .. method .. " to http://" .. host .. ":" .. port .. path .. ": " .. err)
        return nil
    end

    ngx.log(ngx.INFO,"HTTP " .. method .. " request to " .. host .. ":" .. port .. path .. " returned " .. res.status)

    if res.status >= 200 and res.status < 300 then
        return true, res.body
    else
        return false, nil
    end
end

-- Check if an object exists on a remote host
function M.object_exists_on_remote_host(host, port, bucket, object)
    local method = "HEAD"
    local path = "/" .. object .. "?bucket=" .. bucket
    local headers = {}
    local timeout = 1000
    headers['user-agent'] = "scs internal"

    local res = M.http_request(host, port, headers, method, path, timeout)
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
    local timeout = 500
    headers['x-status'] = true
    headers['user-agent'] = "scs internal"

    return M.http_request(host, port, headers, method, path, timeout)
end

-- Create a consistent hash of the values given in a table
function M.create_hash_map(values)
    local hash_map = Flexihash.New()
    for _,value in pairs(values) do
        ngx.log(ngx.DEBUG,"Adding value " .. value .. " of type " .. type(value) .. " to the hash")
        hash_map:addTarget(value)
    end
    return hash_map
end

function M.generate_url(host, port, object, bucket)
    local url
    if port == 80 then
        url = "http://" .. host .. "/" .. object .. "?bucket=" .. bucket
    else
        url = "http://" .. host .. ":" .. port .. "/" .. object .. "?bucket=" .. bucket
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
        local path = "/etc/scs/scs.conf"
        json = read_file(path, true)
        c:set('conf', json)
        ngx.log(ngx.INFO, "Caching: Configuration")
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
        end
    end
    
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
                ngx.log(ngx.INFO,"Caching: Site " .. h['site'] .. " is one of our sites")
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

function M.sync_object(host, bucket, object, filename)
    local dir = M.get_storage_directory()
    local object_base64 = ngx.encode_base64(object)
    local depth = M.get_directory_depth(object)
    
    local cmd="cd " .. dir .. " && /usr/bin/rsync -RzSut " .. bucket .. "/" .. depth .. "/" .. object_base64 .. "/" .. filename .. " rsync://" .. host .. "/scs"
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
    -- TODO: Should store the hash map in memory to avoid having to create it 
    -- for each request.
    local sites = M.get_all_sites()
    local hash_map = M.create_hash_map(sites)

    -- Now we have a hash map, either created or read from memory. Use it to
    -- figure out which sites to use for this object.
    local hash = bucket .. object
    local replicas = M.get_replica_sites()
    local result = M.look_up_hash_map(hash, hash_map, replicas)
    return result
end

function M.get_directory_depth(object)
    local md5 = ngx.md5(object)
    local dir = false
    if md5 then
        local m, err = ngx.re.match(md5, "^(..)(..)",'j')
        if m then
            if #m == 2 then
                dir = m[1] .. "/" .. m[2]
            end
        end
    end
    return dir 
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

-- Check if the request matches one of the hosts in the given table
function M.object_fits_on_this_host(hosts)
    for _,host in pairs(hosts) do
        if ngx.req.get_headers()["Host"] == host then
            return true
        end
    end
    return false
end

-- Check whether the request is an internal scs request (true) or not (false)
function M.is_internal_request(useragent)
    if useragent then
        if useragent == "scs internal" then
            return true
        end
    end
    return false
end

-- Populate the request table with sanitized input data
--function M.parse_request()
--    local h = ngx.req.get_headers()
--    local internal = M.is_internal_request(h['user-agent'])
--    local debug = h['x-debug']
--    local status = h['x-status']
--    local object_md5 = h['x-md5']
--
--    -- Check the md5 content
--    if object_md5 then
--        if not ngx.re.match(object_md5, '^[a-f0-9]+$','j') then
--            ngx.log(ngx.ERR,"Request md5 header contains non-valid characters")
--            object_md5 = nil
--        end
--
--        -- Check the md5 length
--        if not #object_md5 == 32 then
--            ngx.log(ngx.ERR,"Request md5 header length is invalid")
--            object_md5 = nil
--        end
--    end
--
--    local args = ngx.req.get_uri_args()
--
--    -- The bucket is the value of the argument bucket, or the hostname in the
--    -- host header.
--    local bucket = nil
--    if args['bucket'] then
--        bucket = args['bucket']
--    else
--        local m, err = ngx.re.match(ngx.var.host, '^([^.]+)','j')
--        if m then
--            if #m == 1 then
--                bucket = m[1] 
--            end
--        end
--    end
--
--    -- Read the object name, and remove the first char (which is a /)
--    local object = string.sub(ngx.var.uri, 2)
--
--    -- Unescape the filename of the object before hashing
--    object = ngx.unescape_uri(object)
--
--    -- Set both the object and object_base64 to nil if the length of the object
--    -- name is 0.
--    local object_base64 = nil
--    local object_name_md5 = nil
--    if #object > 0 then
--        object_base64 = ngx.encode_base64(object)
--        object_name_md5 = ngx.md5(object)
--    else
--        -- Do not allow 0 character object names
--        object = nil
--    end
--
--    -- Make sure that the bucket name is valid
--    if not M.verify_bucket(bucket) then
--        bucket = false
--    end
--
--    local r = {
--        -- Plain text name of the object
--        ['object'] = object,
--        -- The md5 checksum of the body, given by the client
--        ['object_md5'] = object_md5,
--        -- MD5 checksum of text name of the object
--        ['object_name_md5'] = object_name_md5,
--        -- Base64 name of the object
--        ['object_base64'] = object_base64,
--        -- Relative d/i/r/ectory to use in the file system
--        ['dir'] = M.get_directory_depth(object),
--        -- Request method (HEAD, GET, POST, ..)
--        ['method'] = ngx.var.request_method, 
--        -- Name of the bucket
--        ['bucket'] = bucket, 
--        -- True if the request is an internal scs request
--        ['internal'] = internal, 
--        -- Add debug information in the response
--        ['status'] = status, 
--        -- Add debug information in the response
--        ['debug'] = debug, 
--        ['prefix'] = M.get_parameter('prefix',args), 
--        ['max-keys'] = M.get_parameter('max-keys',args), 
--        ['marker'] = M.get_parameter('marker',args), 
--    }
--
--    -- Clean up
--    ngx.header['Server'] = 'scs'
--    return r
--end

-- Given a directory, return a table with information about each file in that
-- directory - recusively and sorted by mtime.
-- function M.scandir(bucket)
--     -- If the directory does not exist, return an empty table here
--     local dir = M.get_storage_directory()
--     local path = dir
-- 
--     if bucket then
--         path = dir .. "/" .. bucket
--     end
-- 
--     if not M.is_file(path) then
--         return {}
--     end
-- 
--     local entry, objects, popen = nil, {}, io.popen
--     local counter = 0
--     for entry in popen('find ' .. path .. ' -type f -printf "%T@\t%s\t%f\t%h\n" | sort -nr'):lines() do
--         local m, err = ngx.re.match(entry, "^([0-9]+)[^\t]+\t([^\t]+)\t([^\t]+)\t" .. dir .. "/([^/]+).*$","j")
--         if m then
--             if #m == 4 then
--                 local n = {}
--                 n['mtime'] = tonumber(m[1])
--                 n['LastModified'] = ngx.http_time(m[1])
--                 n['size'] = tonumber(m[2])
--                 local object = ngx.decode_base64(m[3])
-- 
--                 if n['mtime'] and bucket == m[4] then
--                     objects[object] = n
--                 end
--             end
--         end
--     end
--     --ngx.log(ngx.ERR,"Scanned the directory " .. path .. " and found " .. #objects .. " objects.")
--     return objects
-- end

function M.replicate_object(hosts, bucket, object, filename)
    -- Replicate the object to other hosts here.
    local status = false
    local count = 0
    for _,host in pairs(hosts) do
        if M.get_host_status(host) then
            local res = M.sync_object(host, bucket, object, filename)
            if res then
                ngx.log(ngx.NOTICE,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " succeeded.")
                count = count + 1
            else
                ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " failed.")
            end
        else
            ngx.log(ngx.WARN,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " not initiated. The host is down.")
        end
    end
    if count >= (#hosts/2) then
        status = true
        ngx.log(ngx.INFO,"Successfully replicated to " .. count .. " hosts. That is more or equal than " .. #hosts/2)
    else
        status = false
        ngx.log(ngx.ERR,"Managed to replicate to " .. count .. " hosts. That is less replicas than " .. #hosts/2 .. ", so report the write as failed.")
    end
    return status
end

function M.get_local_object_path(bucket, object)
    local dir = M.get_storage_directory()
    local path = nil

    if not bucket then
        ngx.log(ngx.ERR,"Bucket not set")
        return nil
    end

    if not object then
        ngx.log(ngx.ERR,"Object not set")
        return nil
    end

    if dir then
        local object_base64 = ngx.encode_base64(object)
        local depth = M.get_directory_depth(object)
        if object_base64 and depth then
             path = dir .. "/" .. bucket .. "/" .. depth .. "/" .. object_base64
        end
    end
    return path
end

function M.get_local_object_versions(bucket, object)
    local path = M.get_local_object_path(bucket, object)
    local versions = {}

    local entry, versions, popen = nil, {}, io.popen
    if M.is_file(path) then
        for entry in popen('find ' .. path .. ' -type f -printf "%T@\t%s\t%f\n" | sort -nr'):lines() do
            local m, err = ngx.re.match(entry, "^([0-9]+)[^\t]+\t([0-9]+)\t([0-9]+)-([a-f0-9]+).data$","j")
            if m then
                if #m == 4 then
                    local n = {}
                    n['mtime'] = tonumber(m[1])
                    n['size'] = tonumber(m[2])
                    n['version'] = tonumber(m[3])
                    n['md5'] = m[4]
                    table.insert(versions, n)
                end
            end
        end
    end
    ngx.log(ngx.INFO,"Found " .. #versions .. " versions locally of " .. bucket .. "/" .. object)
    return versions
end

-- Function to fetch host status and update the cached status for all hosts
-- found in the configuration.
function M.update_status_for_all_hosts(sites)
    sites = M.randomize_table(sites)
    local port = M.get_bind_port()
    local unavailable = {}
    local total_hosts = 0
    for i,site in ipairs(sites) do
        local hosts = M.get_site_hosts(site)
        hosts = M.randomize_table(hosts)
        total_hosts = total_hosts + #hosts
        for i,host in ipairs(hosts) do
            local status = M.remote_host_availability(host, port)
            -- Status is true or false to indicate if the host is
            -- available or not.
            M.update_host_status(host,status)

            if not status then
                table.insert(unavailable,host)
            end
        end
    end

    if #unavailable > 0 then
        ngx.log(ngx.DEBUG,#unavailable .. " of the " .. total_hosts .. " hosts are unavailable: " .. tostring(unavailable))
    end
end

-- Verify the checksum of the file. Return true if valid, false if corrupt.
function M.is_checksum_valid(bucket, object, version, md5)
    local path = M.get_local_object_path(bucket, object)
    local f = path .. "/" .. version .. "-" .. md5 .. ".data"

    local valid = false
    --if M.is_file(f) then
         -- Calculate checksum here
         -- ngx.log(ngx.ERR,"Calculate checksum of " .. f)
         -- local current_checksum = md5(f)
         -- if current_checksum == md5 then
         --     valid = true
         -- else
         --     valid = false
         --     ngx.log(ngx.ERR,"File " .. f .. " is corrupt. Checksum mismatch")
         -- end
    --end

    -- TODO: Remove when the checksum thing has been written.
    valid = true
    return valid
end

function M.full_replication()
    local path = M.get_storage_directory()
    local entry, versions, popen = nil, {}, io.popen
    for entry in popen('find ' .. path .. ' -type f -printf "%T@\t%s\t%f\t%h\n" | sort -nr'):lines() do
        local m, err = ngx.re.match(entry, "^([0-9]+)[^\t]+\t([0-9]+)\t([0-9]+)-([a-f0-9]+).([a-z]+)\t" .. path .. "/([^/]+).*/([^/]+)$","j")
        if m then
            if #m == 7 then
                local mtime = tonumber(m[1])
                local size = tonumber(m[2])
                local version = tonumber(m[3])
                local md5 = m[4]
                local filetype = m[5]
                local bucket = m[6]
                local object_base64 = m[7]
                local object = ngx.decode_base64(object_base64)
                local filename = version .. "-" .. md5 .. "." .. filetype

                -- ngx.log(ngx.ERR,"mtime: " .. m[1] .. ", size: " .. m[2] .. ", version: " .. m[3] .. ", md5: " .. m[4] .. ", type: " .. m[5] .. ", bucket: " .. m[6])

                if filetype == "data" then
                    local valid = M.is_checksum_valid(bucket, object, version, md5)
                    if valid then
                        -- Replicate to other hosts.
                        local sites = M.get_object_replica_sites(bucket, object)
                        local hosts = M.get_replica_hosts(bucket, object, sites)
                        for _,host in pairs(hosts) do
                            if M.get_host_status(host) then
                                local res = M.sync_object(host, bucket, object, filename)
                                if res then
                                    ngx.log(ngx.INFO,"Object " .. bucket .. "/" .. object .. " version " .. version .. " was replicated to " .. host)
                                else
                                    ngx.log(ngx.ERR,"Object " .. bucket .. "/" .. object .. " version " .. version .. " was NOT replicated to " .. host)
                                end
                            else
                                ngx.log(ngx.ERR,host .. " is down. Unable to replicate.")
                            end
                        end
                    else
                        ngx.log(ngx.ERR,"Object " .. bucket .. "/" .. object .. " version " .. version .. " is corrupt")
                        -- M.quarantine(bucket, object, version, md5)
                    end
                -- elseif filetype == "ts" then
                    -- Remove old versions if tombstone exists.
                end
            end
        end
    end
end
    
return M

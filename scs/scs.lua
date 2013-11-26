local common = require "common"
local Request = require "request"
--local Configuration = require "configuration"
local timer = require "timer"
local resty_md5 = require "resty.string.md5"
local str = require "resty.string"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

local function lookup_cluster_status(r)
    local exitcode = ngx.HTTP_OK
    local out = {}
    for host,h in pairs(r.cluster) do
        ngx.log(ngx.DEBUG,"Checking " .. host)
        local status = common.get_host_status(host)
        local weight = h['weight']
        local site = h['site']

        local i = {}
        i['status'] = status
        i['weight'] = weight
        i['site'] = site
        out[host] = i
    end
    return exitcode, out
end

--local function lookup_bucket(r)
--    local exitcode = ngx.HTTP_NOT_FOUND
--    local out = {}
--    -- See if the object exists locally
--    local bucket = r.bucket
--    local internal = r.internal
--    local dir = common.get_storage_directory()
--
--    if not internal then
--        -- Send request to all hosts
--        local method = "GET"
--        local path = "/?bucket=" .. bucket
--        local headers = {}
--        local timeout = 1000
--        local port = common.get_bind_port()
--        headers['user-agent'] = "scs internal"
--
--        local sites = common.get_sites()
--        local entries = 0
--        local size = 0
--        for i,site in ipairs(sites) do
--            local hosts = common.get_site_hosts(site)
--            hosts = common.randomize_table(hosts)
--            for i,host in ipairs(hosts) do
--                local res, body = common.http_request(host, port, headers, method, path, timeout)
--                if res and body then
--                    local e = cjson.decode(body)
--                    entries = entries + #e
--
--                    for i,o in ipairs(e) do
--                        size = size + o['size']
--                    end
--                end
--            end
--        end
--        out['bucket'] = bucket
--        out['entries'] = entries
--        out['bytes'] = size
--        exitcode = ngx.HTTP_OK
--    else
--        -- Read information about the bucket
--        local objects, counters = common.scandir(bucket)
--        out = objects
--        --out = {}
--        --out = counters
--
--        --for i,entry in pairs(entries) do
--        --    --out['foo'] = 'bar'
--        --    --table.insert(out, "i: " .. i .. ", value: " .. value)
--        --    --for k,value in pairs(entry) do
--        --    --    table.insert(out, "k: " .. k .. ", value: " .. value)
--        --    --end
--        --end
--        exitcode = ngx.HTTP_OK
--    end
--    return exitcode, out
--end
--

local function push_queue(r)
    local bucket = r.bucket
    local object = r.object
    local hosts = r.hosts
    local object_base64 = r.object_base64
    local version = ngx.time()
    local queue = r.queue
    if not os.rename(queue, queue) then
        ngx.log(ngx.DEBUG,"Trying to create directory " .. queue)  
        os.execute('mkdir --mode=0755 --parents ' .. queue)
    end

    local out = {}
    out['bucket'] = bucket
    out['object'] = object
    out['object_base64'] = object_base64
    local path = r.objects .. "/" .. r.dir
    out['base'] = r.objects
    out['path'] = r.dir

    for host,_ in pairs(hosts) do
        if ngx.req.get_headers()["Host"] == host then
            -- Pass
        else
            out['host'] = host
            local filename = version .. "-" .. math.random(10000,99999)
            file = io.open(queue .. "/" .. filename .. "-creating", 'w')
            if file then
                file:write(cjson.encode(out))
                file:close()
                ngx.log(ngx.DEBUG,"Object " .. object .. " in bucket " .. bucket .. " added to the replicator queue as " .. filename)

                -- Try to avoid race conditions
                if not os.rename(queue .. "/" .. filename .. "-creating", queue .. "/" .. filename) then
                    ngx.log(ngx.ERR,"Unable to rename " .. filename)
                end
            else
                ngx.log(ngx.ERR,"Unable to add object " .. object .. " in bucket " .. bucket .. " to the replicator queue as " .. filename)
            end
        end
    end
end

local function delete_object(r)
    local out = {}
    local hosts = r.hosts
    local object = r.object
    local bucket = r.bucket

    out['hosts'] = hosts
    out['object'] = r.object
    out['bucket'] = r.bucket
    local version = ngx.time()

    local exitcode = ngx.HTTP_SERVICE_UNAVAILABLE

    if common.object_fits_on_this_host(hosts) then
        ngx.log(ngx.DEBUG,"DELETE request that fits locally received, object " .. object .. " in bucket " .. bucket)
        -- local delete is ok

        local dir = r.objects .. "/" .. r.dir 
        if not os.rename(dir, dir) then
            os.execute('mkdir --mode=0755 --parents ' .. dir)
        end

        out['client'] = 'foo'
        json = cjson.encode(out)
        local object_md5 = ngx.md5(json)
        
        local object_name_on_disk = version .. "-" .. object_md5 .. ".ts"
        local p = dir .. "/" .. object_name_on_disk
        ts = io.open(p, 'w')
        if ts then
            ts:write(json)
            ts:close()
            exitcode = ngx.HTTP_OK
            ngx.log(ngx.INFO,"Added tombstone for object " .. object .. " in bucket " .. bucket .. ", " .. p)

            -- Add the object to the relication short list queue
            push_queue(r)
        else
            ngx.log(ngx.ERR,"Unable to write tombstone for object " .. object .. " in bucket " .. bucket .. ", " .. p)
        end
    else
        ngx.log(ngx.DEBUG,"DELETE request that does not fit locally received, object " .. object .. " in bucket " .. bucket)
        local host = nil
        for h,_ in pairs(hosts) do
            if common.get_host_status(h) then
                host = h
            end
        end

        if host == nil then
            out['message'] = 'None of the hosts are available at the moment. Please try again later.'
            ngx.log(ngx.WARN,'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment')
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        else
            -- Redirect to one of the corrent hosts here. 307.
            local port = hosts[host]['port']
            local url = common.generate_url(host,port,object,bucket,version)
            out['message'] = 'local delete is not ok, prepare for redirect'
            ngx.log(ngx.INFO,'Redirecting DELETE request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url)
            ngx.header["Location"] = url
            exitcode = 307
        end
    end
    return exitcode, out
end

local function post_object(r)
    local internal = r.internal
    local bucket = r.bucket
    local object_md5 = r.object_md5
    local object = r.object
    local object_base64 = r.object_base64
    local version = ngx.time()
    local hosts = r.hosts

    if not object_md5 or not r.dir or not r.objects or not hosts then
        ngx.log(ngx.ERR,"Missing information")
        ngx.exit(ngx.HTTP_BAD_REQUEST)
    end

    local object_name_on_disk = version .. "-" .. object_md5 .. ".data"

    local out = {}
    out['hosts'] = hosts
    out['object'] = object
    out['md5'] = r.object_md5
    out['bucket'] = bucket

    local exitcode = ngx.HTTP_SERVICE_UNAVAILABLE

    if common.object_fits_on_this_host(hosts) then
        -- local upload is ok

        local dir = r.objects .. "/" .. r.dir 
        if not os.rename(dir, dir) then
            os.execute('mkdir --mode=0755 --parents ' .. dir)
        end

        local md5 = resty_md5:new()

        ngx.req.read_body()
        local req_body_file = ngx.req.get_body_file()

        if not req_body_file then
            out['message'] = 'No file found in request'
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        if req_body_file == nil then
            out['message'] = 'Request body is nil'
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        tmpfile = io.open(req_body_file)
        realfile = io.open(dir .. "/" .. object_name_on_disk, 'w')

        local size = 8192
        while true do
            local block = tmpfile:read(size)
            if not block then 
                break
            end
            md5:update(block)
            realfile:write(block)
        end
        tmpfile:close()
        realfile:close()
        local calculated_md5 = str.to_hex(md5:final())
        md5:reset()

        if not object_md5 == calculated_md5 then
            out['message'] = "The checksum of the uploaded file (" .. calculated_md5 .. ") is not the same as the client told us (" .. object_md5 .. ")"
            exitcode = ngx.HTTP_BAD_REQUEST
        elseif not common.path_exists(dir .. "/" .. object_name_on_disk) then
            ngx.log(ngx.ERR,'Failed to write object ' .. object .. ' in bucket ' .. bucket .. ' to local file system (' .. dir .. '/' .. object_name_on_disk .. ')')
            out['message'] = 'Failed to write object'
            exitcode = ngx.HTTP_INTERNAL_SERVER_ERROR
        else
            out['message'] = 'The object was uploaded'
            out['md5'] = object_md5
            out['version'] = version

            -- Add the object to the relication short list queue
            push_queue(r)

            ngx.log(ngx.INFO,'The object ' .. object .. ' in bucket ' .. bucket .. ' was written successfully to local file system (' .. dir .. '/' .. object_name_on_disk .. ')')
            exitcode = ngx.HTTP_OK
        end
    else
        local host = nil
        for h,_ in pairs(hosts) do
            if common.get_host_status(h) then
                host = h
            end
        end

        if host == nil then
            out['message'] = 'None of the hosts are available at the moment. Please try again later.'
            ngx.log(ngx.WARN,'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment')
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        else
            -- Redirect to one of the corrent hosts here. 307.
            local port = hosts[host]['port']
            local url = common.generate_url(host,port,object,bucket,version)
            out['message'] = 'local upload is not ok, prepare for redirect'
            ngx.log(ngx.INFO,'Redirecting POST request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url)
            ngx.header["Location"] = url
            exitcode = 307
        end
    end
    return exitcode, out
end

local function lookup_object(r)
    local object = r.object
    local bucket = r.bucket
    local hosts = r.hosts
    local objects = r.objects
    local dir = r.dir
    local version = r.version

    local out = {}
    out['object'] = object
    out['bucket'] = bucket
    out['hosts'] = hosts
    if version then
        out['version'] = version
    end

    if r.meta then
        if dir and objects then
            out['versions'] = common.get_local_object(objects .. '/' .. dir)
        end
        exitcode = ngx.HTTP_OK
    else
        ngx.log(ngx.DEBUG, 'object ' .. object .. ' in bucket ' .. bucket .. ' was not found locally. Check replica hosts')
        -- return 302, 404 or 503 (if no replica hosts are up)
        local host = common.get_host_with_object(hosts, bucket, object, version)
        if host == nil then
            out['message'] = "All the replica hosts for object " .. object .. " in bucket " .. bucket .. " are unavailable. Please try again later."
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        elseif host == false then
            if version then
                out['message'] = "Version " .. version .. " of the object " .. object .. " in bucket " .. bucket .. " does not exist locally or on any of the available replica hosts."
            else
                out['message'] = "The object " .. object .. " in bucket " .. bucket .. " does not exist locally or on any of the available replica hosts."
            end
            exitcode = ngx.HTTP_NOT_FOUND
        else
            -- Rewrite to correct node
            local port = hosts[host]['port']
            local url = common.generate_url(host,port,object,bucket,version)
            out['message'] = 'Redirecting GET request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. "."

            ngx.header["Location"] = url
            exitcode = ngx.HTTP_MOVED_TEMPORARILY
        end
    end
    return exitcode, out
end

--local conf = Configuration()
local exitcode = nil
local out = {}

local r = Request()

--for host, v in pairs(conf.hosts) do
--    ngx.log(ngx.INFO,"Caching: Host " .. host)
--end

-- method
-- object (version)
-- bucket
-- meta

if r.object and r.bucket then
    if r.method == "HEAD" or r.method == "GET" then
        exitcode, out = lookup_object(r)
    elseif r.method == "POST" then
        exitcode, out = post_object(r)
    elseif r.method == "DELETE" then
        exitcode, out = delete_object(r)
    end
elseif r.bucket then
    exitcode = 200
    out['message'] = 'bucket only'
else
    --exitcode = 200
    --out['message'] = 'no bucket and no object'
    exitcode, out = lookup_cluster_status(r)
end

local elapsed = ngx.now() - ngx.req.start_time()
if not ngx.headers_sent then
    ngx.header["x-elapsed"] = elapsed
end

-- Send the exit code to nginx
ngx.status = exitcode

-- We have some output for the client
if out then
    ngx.header["content-type"] = "application/json"
    ngx.say(cjson.encode(out))
end

return ngx.exit(exitcode)

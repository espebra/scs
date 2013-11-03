local common = require "common"
local Request = require "request"
local timer = require "timer"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

-- local function bucket_index(r)
--     local exitcode = ngx.HTTP_NOT_FOUND
--     local msg
--     local internal = r['internal']
--     local bucket = r['bucket']
-- 
--     local max_keys = r['max-keys']
--     if not max_keys then
--         max_keys = 1000
--     end
--     local prefix = r['prefix']
--     local marker = r['marker']
-- 
--     if internal then
--         -- Return a table with the objects on this host
--         local objects = common.scandir(bucket)
--         if objects then
--             ngx.log(ngx.ERR,"Found " .. #objects .. " in the internal request")
--         end
-- 
--         local json = cjson.encode(objects)
--         if json then
--             ngx.print(json)
--             exitcode = ngx.HTTP_OK
--         end
--     else
--         -- Query the replica hosts for a table of objects
--         local conf = common.get_configuration()
--         local method = "GET"
--         local timeout = 10000
--         local path = "/?bucket=" .. bucket .. "&max-keys=" .. max_keys
--         local headers = {}
--         headers['user-agent'] = "scs internal"
-- 
--         local objects = {}
--         for host,h in pairs(conf.current.hosts) do
--             if common.get_host_status(host) then
--                 local port = common.get_bind_port()
--                 local status, body = common.http_request(host, port, headers, method, path, timeout)
--                 if status then
--                     ngx.log(ngx.INFO,"Object list retrieved successfully from " .. host)
--                     local host_objects = cjson.decode(body)
--                     if host_objects then
--                         for host_object,v in pairs(host_objects) do
--                             if not objects[host_object] then
--                                 v['replicas'] = 1
--                                 v['replica_hosts'] = {}
--                                 table.insert(v['replica_hosts'],host)
--                                 objects[host_object] = v
--                             else
--                                 table.insert(objects[host_object]['replica_hosts'],host)
--                                 objects[host_object]['replicas'] = objects[host_object]['replicas'] + 1
--                                 if objects[host_object]['mtime'] < v['mtime'] then
--                                     objects[host_object]['mtime'] = v['mtime']
--                                     objects[host_object]['LastModified'] = v['LastModified']
--                                 end
--                             end
--                         end
--                     end
--                 else
--                     ngx.log(ngx.WARN,"Failed to retrieve the object list from " .. host)
--                 end
--             end
--         end
-- 
--         local res = {}
--         res['bucket'] = bucket
--         if prefix then
--             res['prefix'] = prefix
--         end
--         if marker then
--             res['marker'] = marker
--         end
--         res['contents'] = objects
-- 
--         -- for i,o in pairs(objects) do
--         -- end
-- 
--         local json = cjson.encode(res)
--         if json then
--             ngx.print(json)
--             exitcode = ngx.HTTP_OK
--         end
--     end
--     return exitcode
-- end

local function lookup_object(r)
    local exitcode = ngx.HTTP_NOT_FOUND
    local out = {}
    -- See if the object exists locally
    local object = r.object
    local bucket = r.bucket
    local object_base64 = r.object_base64
    local internal = r.internal

    -- The object do not exist locally
    if not internal then
        -- We do not have the file locally. Should lookup the hash table to
        -- find a valid host to redirect to. 302.
        local sites = common.get_object_replica_sites(bucket, object)
        local hosts = common.get_replica_hosts(bucket, object, sites)

        -- Print replica hosts for this object
        out['hosts'] = hosts

        local host = common.get_host_with_object(hosts, bucket, object)
        if host == nil then
            out['message'] = "All the replica hosts for object " .. object .. " in bucket " .. bucket .. " are unavailable. Please try again later."
            out['success'] = false
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        elseif host == false then
            out['message'] = "The object " .. object .. " in bucket " .. bucket .. " does not exist locally or on any of the available replica hosts."
            out['success'] = false
            exitcode = ngx.HTTP_NOT_FOUND
        else
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object,bucket)
            out['message'] = 'Redirecting GET request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. "."
            out['success'] = true

            ngx.header["Location"] = url
            exitcode = ngx.HTTP_MOVED_TEMPORARILY
        end
    end
    return exitcode, out
end

local function lookup_bucket(r)
    local exitcode = ngx.HTTP_NOT_FOUND
    local out = {}
    -- See if the object exists locally
    local bucket = r.bucket
    local internal = r.internal
    local dir = common.get_storage_directory()

    if not internal then
        -- Send request to all hosts
        local method = "GET"
        local path = "/?bucket=" .. bucket
        local headers = {}
        local timeout = 1000
        local port = common.get_bind_port()
        headers['user-agent'] = "scs internal"

        local sites = common.get_all_sites()
        local entries = 0
        local size = 0
        for i,site in ipairs(sites) do
            local hosts = common.get_site_hosts(site)
            hosts = common.randomize_table(hosts)
            for i,host in ipairs(hosts) do
                local res, body = common.http_request(host, port, headers, method, path, timeout)
                if res and body then
                    local e = cjson.decode(body)
                    entries = entries + #e

                    for i,o in ipairs(e) do
                        size = size + o['size']
                    end
                end
            end
        end
        out['bucket'] = bucket
        out['entries'] = entries
        out['bytes'] = size
        exitcode = ngx.HTTP_OK
    else
        -- Read information about the bucket
        local objects, counters = common.scandir(bucket)
        out = objects
        --out = {}
        --out = counters

        --for i,entry in pairs(entries) do
        --    --out['foo'] = 'bar'
        --    --table.insert(out, "i: " .. i .. ", value: " .. value)
        --    --for k,value in pairs(entry) do
        --    --    table.insert(out, "k: " .. k .. ", value: " .. value)
        --    --end
        --end
        exitcode = ngx.HTTP_OK
    end
    return exitcode, out
end

local function post_object(r)
    local out = {}
    if not r.object_md5 or not r.object or not r.bucket then
        ngx.log(ngx.ERR,"Missing input data")
        ngx.exit(ngx.HTTP_BAD_REQUEST)
    end
    local internal = r.internal
    local bucket = r.bucket
    local object = r.object
    local object_base64 = r.object_base64
    local object_name_on_disk = ngx.time() .. "-" .. r.object_md5 .. ".data"

    local sites = common.get_object_replica_sites(bucket, object)
    local hosts = common.get_replica_hosts(bucket, object, sites)
    local exitcode = ngx.HTTP_SERVICE_UNAVAILABLE

    local dir = common.get_storage_directory()
    if common.object_fits_on_this_host(hosts) then
        local path = common.get_local_object_path(bucket, object)
        if not os.rename(path, path) then
            os.execute('mkdir --mode=0755 --parents ' .. path)
        end

        ngx.req.read_body()
        local req_body_file = ngx.req.get_body_file()

        if not req_body_file then
            out['message'] = 'No file found in request'
            out['success'] = false
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        if req_body_file == nil then
            out['message'] = 'Request body is nil'
            out['success'] = false
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        tmpfile = io.open(req_body_file)
        realfile = io.open(path .. "/" .. object_name_on_disk, 'w')

        local size = 2^20      -- good buffer size (1M)
        while true do
            local block = tmpfile:read(size)
            if not block then 
                break
            end
            realfile:write(block)
        end
        tmpfile:close()
        realfile:close()

        if not common.is_file(path .. "/" .. object_name_on_disk) then
            ngx.log(ngx.ERR,'Failed to write object ' .. object .. ' in bucket ' .. bucket .. ' to local file system (' .. path .. '/' .. object_name_on_disk .. ')')
            out['message'] = 'Failed to write object'
            out['success'] = false
        else
            out['message'] = 'The object was uploaded'
            out['success'] = true

            ngx.log(ngx.INFO,'The object ' .. object .. ' in bucket ' .. bucket .. ' was written successfully to local file system (' .. path .. '/' .. object_name_on_disk .. ')')
            exitcode = ngx.HTTP_OK
        end
    else
        out['hosts'] = hosts

        local host = nil
        local hosts_text = "["
        for _,h in pairs(hosts) do
            if common.get_host_status(h) then
                host = h
            end
            hosts_text = hosts_text .. " " .. h 
        end
        hosts_text = hosts_text .. " ]"

        if host == nil then
            out['success'] = false
            out['message'] = 'None of the hosts are available at the moment. Please try again later.'
            ngx.log(ngx.WARN,'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment ' .. hosts_text)
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        else
            -- Redirect to one of the corrent hosts here. 307.
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object,bucket)
            ngx.header["Location"] = url
            ngx.log(ngx.INFO,'Redirecting POST request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text)
            exitcode = 307
        end
    end
    return exitcode, out
end

local function put_object(internal, bucket, object, req_body_file)
    local exitcode = ngx.HTTP_OK
    ngx.req.read_body()
    local req_body_file = ngx.req.get_body_file()
    return exitcode
end

local function delete_object(internal, bucket, object)
    local exitcode = ngx.HTTP_OK
    return exitcode
end

local r = Request()
local method = r.method

--local r = common.parse_request()
--local method = r['method']

--ngx.header["server"] = nil

local exitcode = nil
local out = {}

if method == "POST" then
    exitcode, out = post_object(r)
elseif method == "PUT" then
    exitcode = put_object(r.internal, r.bucket, r.object)
elseif method == "DELETE" then
    exitcode = delete_object(r.internal, r.bucket, r.object)
elseif method == "GET" or method == "HEAD" then
    -- if r['bucket'] and not r['object'] then
    --     exitcode = bucket_index(r)
    -- elseif r['bucket'] and r['object'] then
    --     exitcode = lookup_object(r)
    -- end
    if not r.bucket then
        exitcode = 200
        out['success'] = false
        out['message'] = "Bucket not specified"
    elseif not r.object then
        exitcode, out = lookup_bucket(r)
    else
        exitcode, out = lookup_object(r)
    end
end

local elapsed = ngx.now() - ngx.req.start_time()
if not ngx.headers_sent then
    if elapsed > 0 then
        ngx.header["x-elapsed"] = elapsed
    end
end

-- The exit code should be set at this point
if not exitcode then
    exitcode = 500
end

-- We have some output for the client
if exitcode == 200 then
    if out then
        ngx.say(cjson.encode(out))
    end
end

ngx.exit(exitcode)


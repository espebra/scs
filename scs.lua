local common = require "scs.common"
local timer = require "scs.timer"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

local function lookup_object(r)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg
    -- See if the object exists locally
    local object = r['object']
    local bucket = r['bucket']
    local object_base64 = r['object_base64']
    local internal = r['internal']
    local dir = common.get_storage_directory()

    -- The object do not exist locally
    if not internal then
        -- We do not have the file locally. Should lookup the hash table to
        -- find a valid host to redirect to. 302.
        local sites = common.get_object_replica_sites(bucket, object)
        local hosts = common.get_replica_hosts(bucket, object, sites)

        -- Easier to understand what is happening when debugging
        local hosts_text = "["
        for _,host in pairs(hosts) do
            hosts_text = hosts_text .. " " .. host
        end
        hosts_text = hosts_text .. " ]"

        local host = common.get_host_with_object(hosts, bucket, object)
        if host == nil then
            msg = "All the replica hosts for object " .. object .. " in bucket " .. bucket .. " are unavailable. Please try again later " .. hosts_text
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        elseif host == false then
            msg = "The object " .. object .. " in bucket " .. bucket .. " does not exist locally or on any of the available replica hosts " .. hosts_text
            exitcode = ngx.HTTP_NOT_FOUND
        else
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object)
            msg = 'Redirecting GET request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
            ngx.header["Location"] = url
            exitcode = ngx.HTTP_MOVED_TEMPORARILY
        end
    end
    return exitcode, msg
end

local function post_object(internal, bucket, object)
    local sites = common.get_object_replica_sites(bucket, object)
    local hosts = common.get_replica_hosts(bucket, object, sites)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg = nil

    local dir = common.get_storage_directory()
    if common.object_fits_on_this_host(hosts) then
        local object_base64 = ngx.encode_base64(object)
        local path = dir .. "/" ..  bucket
        if not os.rename(path, path) then
            os.execute('mkdir -p ' .. path)
        end

        ngx.req.read_body()
        local req_body_file = ngx.req.get_body_file()

        if not req_body_file then
            msg = 'No file found in request'
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        if req_body_file == nil then
            msg = 'Request body is nil'
            exitcode = ngx.HTTP_BAD_REQUEST
        end

        tmpfile = io.open(req_body_file)
        realfile = io.open(path .. "/" .. object_base64, 'w')

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

        if common.object_exists_locally(dir, bucket, object_base64) then
            msg = 'The object ' .. object .. ' in bucket ' .. bucket .. ' was written successfully to local file system.'
            exitcode = ngx.HTTP_OK
        else
            msg = 'Failed to write object ' .. object .. ' in bucket ' .. bucket .. ' to local file system'
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        end

        -- Finish the request here if the configuration is set to write back.
        local write_back = common.get_write_back()
        if write_back then
            ngx.eof()
        end

        -- Replicate the object to other hosts here.
        for _,host in pairs(hosts) do
            if common.get_host_status(host) then
                local res = common.sync_object(dir, host, bucket, object_base64)
                if res then
                    ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " succeeded.")
                else
                    ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " failed.")
                end
            else
                ngx.log(ngx.ERR,"Sync " .. bucket .. "/" .. object .. " to " .. host .. " not initiated. The host is down.")
            end
        end
    else
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
            msg = 'None of the hosts for object ' .. object .. ' in bucket ' .. bucket .. ' are available at the moment ' .. hosts_text
            exitcode = ngx.HTTP_SERVICE_UNAVAILABLE
        else
            -- Redirect to one of the corrent hosts here. 307.
            --ngx.log(ngx.ERR,"Found " .. #hosts .. " available hosts, selected " .. host)
            local port = common.get_bind_port()
            local url = common.generate_url(host,port,object)
            ngx.header["Location"] = url
            msg = 'Redirecting POST request for object ' .. object .. ' in bucket ' .. bucket .. ' to ' .. url .. " " .. hosts_text
            exitcode = 307
        end
    end
    return exitcode, msg
end

local function put_object(internal, bucket, object, req_body_file)
    local msg
    local exitcode = ngx.HTTP_OK
    ngx.req.read_body()
    local req_body_file = ngx.req.get_body_file()
    return exitcode, msg
end

local function delete_object(internal, bucket, object)
    local msg
    local exitcode = ngx.HTTP_OK
    return exitcode, msg
end

local r = common.parse_request()
--ngx.header["server"] = nil

local exitcode = nil
local msg = nil

--exitcode, msg = route.request(r)
local method = r['method']
if method == "POST" then
    exitcode, msg = post_object(r['internal'], r['bucket'], r['object'])
elseif method == "PUT" then
    exitcode, msg = put_object(r['internal'], r['bucket'], r['object'])
elseif method == "DELETE" then
    exitcode, msg = delete_object(r['internal'], r['bucket'], r['object'])
elseif method == "GET" or method == "HEAD" then
    exitcode, msg = lookup_object(r)
end

local elapsed = ngx.now() - ngx.req.start_time()
if not ngx.headers_sent then
    if elapsed > 0 then
        ngx.header["x-elapsed"] = elapsed
    end
end

if debug then
    if msg then
        ngx.log(ngx.ERR, "Req time: " .. elapsed .. " sec. " .. msg)
    else
        ngx.log(ngx.ERR, "Req time: " .. elapsed .. " sec. No message")
    end
end
ngx.exit(exitcode)

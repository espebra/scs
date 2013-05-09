local common = require "scs.common"
local timer = require "scs.timer"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

local function rewrite_request(r)
    local exitcode = ngx.HTTP_NOT_FOUND
    local msg
    -- See if the object exists locally
    local object = r['object']
    local bucket = r['bucket']
    local object_base64 = r['object_base64']
    local dir = common.get_storage_directory()
    if common.object_exists_locally(dir, bucket, object_base64) then
        --local uri = "/" .. bucket .. "/" .. object_base64
        local uri = "/" .. bucket .. "/" .. object_base64
        ngx.log(ngx.ERR,"Rewriting URI " .. ngx.var.uri .. " to " .. uri)
        ngx.req.set_uri(uri,true)
    else
        -- The object do not exist locally
        if ngx.is_subrequest then
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
    end
    ngx.exit(exitcode)
end

ngx.header["server"] = nil
local h = ngx.req.get_headers()
local internal = common.is_internal_request(h['user-agent'])
local debug = h['x-debug']
local status = h['x-status']
local bucket = h['x-bucket']

local exitcode = nil
local msg = nil

-- Return 200 immediately if the x-status header is set. This is to verify that
-- the host is up and running.
if status and internal then
    exitcode = ngx.HTTP_OK
    msg = "Returning 200 to the status check."
end

if not status and not bucket then
    exitcode = ngx.HTTP_BAD_REQUEST
    msg = "The request is missing the x-bucket header."
end

if not status and not common.verify_bucket(bucket) then
    exitcode = ngx.HTTP_BAD_REQUEST
    if bucket == nil then
        msg = "Invalid bucket name."
    else
        msg = "Invalid bucket name (" .. bucket .. ")."
    end
end

-- Read the object name, and remove the first char (which is a /)
local object = string.sub(ngx.var.request_uri, 2)
if not status and string.len(object) == 0 then
    exitcode = ngx.HTTP_BAD_REQUEST
    msg = "The object name is not set."
end

-- Unescape the filename of the object before hashing
object = ngx.unescape_uri(object)

-- Assemble the request metadata table
local r = {
  ['object'] = object, -- Plain text name of the object
  ['object_base64'] = ngx.encode_base64(object), -- Base64 name of the object
  ['method'] = ngx.var.request_method, -- Request method (HEAD, GET, POST, ..)
  ['bucket'] = bucket, -- Name of the bucket
  ['internal'] = internal, -- True if internal signaling request
  ['debug'] = debug, -- Add debug information in the response
}

if not exitcode then
    local method = r['method']
    if method == "GET" or method == "HEAD" then
        rewrite_request(r)
    end
end


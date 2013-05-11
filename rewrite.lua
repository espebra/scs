local common = require "scs.common"
local timer = require "scs.timer"

--local http = require "libs.resty.http.simple"
--local Flexihash = require 'libs.Flexihash'
local cjson = require 'cjson'

local function rewrite_request(r)
    local exitcode
    local msg
    -- See if the object exists locally
    local object = r['object']
    local bucket = r['bucket']
    local object_base64 = r['object_base64']
    local internal = r['internal']
    local dir = common.get_storage_directory() .. r['dir']
    if common.object_exists_locally(dir, bucket, object_base64) then
        local uri = "/" .. bucket .. "/" .. object_base64
        ngx.log(ngx.ERR,"Found " .. bucket .. "/" .. object .. " in local file system")
        ngx.log(ngx.ERR,"Rewriting URI " .. ngx.var.uri .. " to " .. uri)
        ngx.req.set_uri(uri, true)
    end
end

local r = common.parse_request()
timer.initiate_periodic_health_checks(1)

-- Return 200 to the status check
if r['status'] and r['internal'] then
    ngx.exit(ngx.HTTP_OK)
end

local method = r['method']
if method == "GET" or method == "HEAD" then
    rewrite_request(r)
end


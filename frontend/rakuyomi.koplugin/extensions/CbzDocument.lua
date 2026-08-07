local PdfDocument = require('document/pdfdocument')
local logger = require("logger")
local Backend = require("Backend")

local function getString(metadata, key)
  local value = metadata[key]
  if type(value) == "string" then
    local trimmed = value:match("^%s*(.-)%s*$")
    return trimmed ~= "" and trimmed or nil
  end
  return nil
end

local function getNumber(metadata, key)
  local value = metadata[key]
  if type(value) == "number" then
    return value
  elseif type(value) == "string" then
    return tonumber(value)
  end
  return nil
end

local CbzDocument = PdfDocument:extend {
  -- Inherit properties and methods from PdfDocument
}

function CbzDocument:getDocumentProps()
  local base_props = PdfDocument.getDocumentProps(self)

  local metadata = self:_getComicBookInfoFromBackend()
  if not metadata then
    return base_props
  end

  local info = self:_mapMetadata(metadata)

  -- Merge the parsed metadata with the base properties
  for key, value in pairs(info) do
    base_props[key] = value
  end

  return base_props
end

--- Asks the already-running backend server to read the CBZ's ComicInfo.xml.
--- @private
--- @return table|nil The simplified metadata table, or nil if unavailable.
function CbzDocument:_getComicBookInfoFromBackend()
  local file_path = self.file

  local ok, response = pcall(Backend.getCbzMetadata, file_path)
  if not ok then
    logger.warn("CbzDocument: Failed to request metadata from backend:", response)
    return nil
  end

  if response.type ~= 'SUCCESS' then
    logger.dbg("CbzDocument: Backend returned no metadata for", file_path, ":", response.message)
    return nil
  end

  return response.body
end

--- Maps the backend's simplified metadata table into KOReader's document props shape.
--- @private
--- @param metadata table The simplified metadata table returned by the backend.
--- @return table The mapped document props.
function CbzDocument:_mapMetadata(metadata)
  local info = {}

  info.title = getString(metadata, "title")
  info.series = getString(metadata, "series")
  info.publisher = getString(metadata, "publisher")
  info.notes = getString(metadata, "notes")
  info.language = getString(metadata, "language")
  info.keywords = getString(metadata, "keywords")
  info.author = getString(metadata, "authors")
  info.series_index = getNumber(metadata, "series_index")

  local rating = getNumber(metadata, "rating")
  if rating and rating >= 0 then
    info.rating = rating
  end

  local pub_year = getNumber(metadata, "publication_year")
  if pub_year then
    info.publication_year = pub_year
  end

  return info
end

function CbzDocument:register(registry)
  registry:addProvider("cbz", "application/vnd.comicbook+zip", self, 110)
end

return CbzDocument

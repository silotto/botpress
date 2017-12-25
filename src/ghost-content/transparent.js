/*
  Transparent Ghost Content Manager hs the same API but
  proxies all calls directly to the FS.
  It's used while in development.
*/

import path from 'path'
import fs from 'fs'
import Promise from 'bluebird'
import glob from 'glob'

import { normalizeFolder as _normalizeFolder } from './util'

Promise.promisifyAll(fs)

module.exports = ({ logger, projectLocation }) => {
  const normalizeFolder = _normalizeFolder(projectLocation)

  logger.info('[Ghost Content Manager] (transparent) Initialized')

  return {
    addRootFolder: (rootFolder, filesGlob) => {
      const { normalizedFolderName } = normalizeFolder(rootFolder)
      logger.debug(`[Ghost Content Manager] (transparent) Added root folder ${normalizedFolderName}, doing nothing.`)
    },
    recordRevision: (folder, file, content) => {
      const { folderPath } = normalizeFolder(folder)
      const filePath = path.join(folderPath, file)
      return fs.writeFileAsync(filePath, content)
    },
    readFile: (folder, file) => {
      const { folderPath } = normalizeFolder(folder)
      const filePath = path.join(folderPath, file)
      return fs.readFileAsync(filePath, 'utf-8')
    },

    deleteFile: (folder, file) => {
      const { folderPath } = normalizeFolder(folder)
      const filePath = path.join(folderPath, file)
      return Promise.fromCallback(cb => fs.unlink(filePath, cb))
    },

    directoryListing: (rootFolder, fileEndingPattern = '') => {
      const flowsDir = normalizeFolder(rootFolder)
      if (!fs.existsSync(flowsDir)) {
        return Promise.resolve([])
      }

      return Promise.fromCallback(cb => glob(`**/*.${fileEndingPattern}`, { cwd: flowsDir }, cb))
    },
    getPending: () => ({}),
    getPendingWithContent: () => ({})
  }
}

import path from 'path'
import fs from 'fs'
import glob from 'glob'
import _ from 'lodash'
import Promise from 'bluebird'
import EventEmitter2 from 'eventemitter2'
import mkdirp from 'mkdirp'

import { validateFlowSchema } from './validator'

export default class FlowProvider extends EventEmitter2 {
  constructor({ logger, botfile, projectLocation, ghostManager }) {
    super({
      wildcard: true,
      maxListeners: 100
    })

    this.logger = logger
    this.botfile = botfile
    this.projectLocation = projectLocation
    this.ghostManager = ghostManager
    this.flowsDir = this.botfile.flowsDir || './flows'

    mkdirp.sync(path.dirname(this.flowsDir))
    this.ghostManager.addRootFolder(this.flowsDir, '**/*.json')
  }

  async loadAll() {
    const flowFiles = await this.ghostManager.directoryListing(this.flowsDir, '.flow.json')

    const flows = await Promise.all(
      flowFiles.map(async name => {
        const uiFileName = name.replace(/\.flow/g, '.ui')
        const flow = JSON.parse(await this.ghostManager.readFile(this.flowsDir, name))

        const schemaError = validateFlowSchema(flow)
        if (!flow || schemaError) {
          return flow ? this.logger.warn(schemaError) : null
        }

        const uiEq = JSON.parse(await this.ghostManager.readFile(this.flowsDir, uiFileName))

        Object.assign(flow, { links: uiEq.links })

        // Take position from UI files or create default position
        flow.nodes.forEach(node => {
          const uiNode = _.find(uiEq.nodes, { id: node.id }) || {}
          Object.assign(node, uiNode.position)
        })

        const unplacedY = (_.maxBy(flow.nodes, 'y') || { y: 0 }).y + 250
        flow.nodes.filter(node => _.isNil(node.x) || _.isNil(node.y)).forEach((node, i) => {
          node.y = unplacedY
          node.x = 50 + i * 250
        })

        return {
          name,
          location: name,
          nodes: _.filter(flow.nodes, node => !!node),
          ..._.pick(flow, 'version', 'catchAll', 'startNode', 'links', 'skillData')
        }
      })
    )

    return flows.filter(flow => Boolean(flow))
  }

  async saveFlows(flows) {
    const flowsToSave = await Promise.mapSeries(flows, flow => this._prepareSaveFlow(flow))

    for (const { flowPath, uiPath, flowContent, uiContent } of flowsToSave) {
      this.ghostManager.recordRevision(this.flowsDir, flowPath, JSON.stringify(flowContent, null, 2))
      this.ghostManager.recordRevision(this.flowsDir, uiPath, JSON.stringify(uiContent, null, 2))
    }

    const flowFiles = await this.ghostManager.directoryListing(this.flowsDir, '.json')
    flowFiles
      .filter(filePath => !flowsToSave.find(flow => flow.flowPath === filePath || flow.uiPath === filePath))
      .map(filePath => {
        this.ghostManager.deleteFile(this.flowsDir, filePath)
      })

    this.emit('flowsChanged')
  }

  async _prepareSaveFlow(flow) {
    flow = Object.assign({}, flow, { version: '0.1' })

    const schemaError = validateFlowSchema(flow)
    if (schemaError) {
      throw new Error(schemaError)
    }

    // What goes in the ui.json file
    const uiContent = {
      nodes: flow.nodes.map(node => ({ id: node.id, position: { x: node.x, y: node.y } })),
      links: flow.links
    }

    // What goes in the .flow.json file
    const flowContent = {
      ..._.pick(flow, 'version', 'catchAll', 'startNode', 'skillData'),
      nodes: flow.nodes.map(node => _.omit(node, 'x', 'y', 'lastModified'))
    }

    const flowPath = flow.location
    const uiPath = flowPath.replace(/\.flow\.json/i, '.ui.json')

    return { flowPath, uiPath, flowContent, uiContent }
  }
}

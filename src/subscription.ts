import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const TI_DIDS = [
  'did:plc:vtwv2edeyhasfqgd53x5yius',	// theintercept.com
  'did:plc:v66uows7g6rrgrrdnwlaxzxf',	// alyxaundria.bsky.social
  'did:plc:i5sk7zx23dffzqsxafl76hqw',	// natashalennard.bsky.social
  'did:plc:lbq3fne6i7blcj3tv7d3k4n3',	// lilianasegura.bsky.social
  'did:plc:cef3vtgzweyj6scaovjcf2be',	// jeremyscahill.com
  'did:plc:45rdujsiek4xlpc7v7tvmvsn',	// drboguslaw.bsky.social
  'did:plc:5klb3ssuxgjnhbwy6b3mztmo',	// schuylermitchell.bsky.social
  'did:plc:y2irhuebp6roxzlq7xhlvwsw',	// hedtk.theintercept.com
  'did:plc:iyfsqmq4tv7opws5ixjsc3o3',	// ali-gharib.theintercept.com
  'did:plc:pqedcubzk3n7ep56fiwaokb4',	// jordansmith.bsky.social
  'did:plc:bd4jsw5jphamhymzjrvjj23u',	// sambiddle.bsky.social
  'did:plc:63uin4tkxta77wpnifmdkkug',	// rodrigobrandao.bsky.social
  'did:plc:xoj5fvglhzfo5xeozvmdvopl',	// ryangrim.bsky.social
  'did:plc:4k6dakv7cskxttdvfpzadq7e',	// kenklippenstein.bsky.social
  'did:plc:uzucgmkg2y5vbkqnbv2on7g3',	// micahflee.com
  'did:plc:hcvylxdvglpgz4t6754s46id',	// nausicaa.bsky.social
  'did:plc:ckfo6fu3limwhudbr3i4l2zb'	// akil.bsky.social
];

function isInterceptStaff(author) {
  return TI_DIDS.includes(author);
}

function isInterceptArticle(record) {
  return record.text.toLowerCase().includes('https://theintercept.com');
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)


    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only ti authors and articles.
        return isInterceptArticle(create.record) || isInterceptStaff(create.author);
      })
      .map((create) => {
        // map ti-related posts to a db row
        console.log({
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        });
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}

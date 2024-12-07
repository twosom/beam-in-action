import {PubSub} from '@google-cloud/pubsub';
import {v4 as uuidv4} from 'uuid';
import faker from 'faker';
import * as yargs from 'yargs';

interface Transaction {
    uid: string;
    item: string;
    price: number;
    transaction_id: string;
    transaction_ts: number;
}

// Command line arguments parsing
const argv = yargs
    .option('msg', {
        alias: 'm',
        description: 'Messages to send',
        type: 'number',
        default: 10000
    })
    .option('sleep', {
        alias: 's',
        description: 'Thread sleep between messages (milliseconds)',
        type: 'number',
        default: 50
    })
    .option('topic', {
        alias: 't',
        description: 'Pub/Sub topic',
        type: 'string',
        default: 'transactions-input'
    })
    .option('project', {
        alias: 'p',
        description: 'Google project',
        type: 'string',
        default: 'project-name'
    })
    .option('workers', {
        alias: 'w',
        description: 'Number of parallel workers',
        type: 'number',
        default: 20
    })
    .option('mode', {
        description: 'Operation mode (console or pubsub)',
        type: 'string',
        default: 'console',
        choices: ['console', 'pubsub']
    })
    .help()
    .argv;

const getEnv = (key: string, fallback: string): string => {
    return process.env[key] || fallback;
};

// Generate a single transaction
function generateTransaction(): Transaction {
    return {
        uid: `uid_${faker.datatype.number({min: 1, max: 2000})}`,
        item: faker.commerce.product(),
        price: parseFloat(faker.commerce.price(1, 20)),
        transaction_id: uuidv4(),
        transaction_ts: Date.now()
    };
}

// Console mode publisher
async function publishToConsole(
    workerUuid: string,
    messages: number,
    sleep: number
): Promise<void> {
    for (let i = 0; i < messages; i++) {
        const transaction = generateTransaction();
        console.log(`[${workerUuid}][${i}]:`, JSON.stringify(transaction, null, 2));
        await new Promise(resolve => setTimeout(resolve, sleep));
    }
    console.log(`Worker ${workerUuid} done`);
}

// Pub/Sub mode publisher
async function publishToPubSub(
    workerUuid: string,
    messages: number,
    sleep: number,
    projectId: string,
    topicId: string
): Promise<void> {
    const pubsub = new PubSub({projectId});
    const topic = pubsub.topic(topicId);

    for (let i = 0; i < messages; i++) {
        const transaction = generateTransaction();
        const messageBuffer = Buffer.from(JSON.stringify(transaction));
        try {
            const messageId = await topic.publish(messageBuffer);
            console.log(`[${workerUuid}][${i}]: Message ${messageId} published.`);
            await new Promise(resolve => setTimeout(resolve, sleep));
        } catch (error) {
            console.error(`Error publishing message: ${error}`);
            throw error;
        }
    }

    console.log(`Worker ${workerUuid} done`);
}

async function main(): Promise<void> {
    const podUuid = uuidv4();
    console.log(`pod ${podUuid} started!`);


    // @ts-ignore
    let {msg: messages, sleep, topic: topicId, project: projectId, workers, mode} = argv;

    if (mode === 'pubsub') {
        const envTopic = getEnv('TOPIC_ID', 'nil');
        if (envTopic !== 'nil') {
            topicId = envTopic;
            console.log(`using topic id from env variable: ${topicId}`);
        }

        const envProject = getEnv('PROJECT_ID', 'nil');
        if (envProject !== 'nil') {
            projectId = envProject;
            console.log(`using project id from env variable: ${projectId}`);
        }
    }

    console.log(
        `Mode: ${mode}\n` +
        `Sending ${messages} messages each of ${workers} workers` +
        (mode === 'pubsub' ? ` to the topic ${topicId} in the project ${projectId}` : '') +
        ` with latency ${sleep} milliseconds`
    );

    const workerPromises = Array(workers)
        .fill(null)
        .map(() => {
            const workerUuid = uuidv4();
            return mode === 'console'
                ? publishToConsole(workerUuid, messages, sleep)
                : publishToPubSub(workerUuid, messages, sleep, projectId, topicId);
        });

    try {
        await Promise.all(workerPromises);
        console.log(`pod ${podUuid} done`);
    } catch (error) {
        console.error('Error in workers:', error);
        process.exit(1);
    }
}

main().catch(console.error);

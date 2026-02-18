use itertools::Itertools;
use std::collections::HashMap;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};

use substreams_helper::{event_handler::EventHandler, hex::Hexable};

use crate::{abi, store_key::StoreKey, traits::PoolAddresser};
use tycho_substreams::prelude::*;

// Auxiliary struct to serve as a key for the HashMaps.
#[derive(Clone, Hash, Eq, PartialEq)]
struct ComponentKey<T> {
    component_id: String,
    name: T,
}

impl<T> ComponentKey<T> {
    fn new(component_id: String, name: T) -> Self {
        ComponentKey { component_id, name }
    }
}

#[derive(Clone)]
struct PartialChanges {
    transaction: Transaction,
    entity_changes: HashMap<ComponentKey<String>, Attribute>,
    balance_changes: HashMap<ComponentKey<Vec<u8>>, BalanceChange>,
}

impl PartialChanges {
    fn consolidate_entity_changes(self) -> Vec<EntityChanges> {
        self.entity_changes
            .into_iter()
            .map(|(key, attribute)| (key.component_id, attribute))
            .into_group_map()
            .into_iter()
            .map(|(component_id, attributes)| EntityChanges { component_id, attributes })
            .collect()
    }
}

#[substreams::handlers::map]
pub fn map_pool_events(
    block: eth::Block,
    block_entity_changes: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockChanges, substreams::errors::Error> {
    let mut block_entity_changes = block_entity_changes;
    let mut tx_changes: HashMap<Vec<u8>, PartialChanges> = HashMap::new();

    handle_events(&block, &mut tx_changes, &pools_store);
    merge_block(&mut tx_changes, &mut block_entity_changes);

    Ok(block_entity_changes)
}

fn handle_events(
    block: &eth::Block,
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    store: &StoreGetProto<ProtocolComponent>,
) {
    let mut eh = EventHandler::new(block);
    eh.filter_by_address(PoolAddresser { store });

    // V2 Sync Handler
    let mut on_sync = |event: abi::pair::events::Sync, _tx: &eth::TransactionTrace, _log: &eth::Log| {
        let pool_address_hex = _log.address.to_hex();
        let pool = store.must_get_last(StoreKey::Pool.get_unique_pool_key(pool_address_hex.as_str()));
        let reserves = [event.reserve0, event.reserve1];

        let tx_change = tx_changes.entry(_tx.hash.clone()).or_insert_with(|| PartialChanges {
            transaction: _tx.into(),
            entity_changes: HashMap::new(),
            balance_changes: HashMap::new(),
        });

        for (i, reserve) in reserves.iter().enumerate() {
            let name = format!("reserve{}", i);
            tx_change.entity_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), name.clone()),
                Attribute { name, value: reserve.clone().to_signed_bytes_be(), change: ChangeType::Update.into() },
            );
        }

        for (i, token) in pool.tokens.iter().enumerate() {
            tx_change.balance_changes.insert(
                ComponentKey::new(pool_address_hex.clone(), token.clone()),
                BalanceChange {
                    token: token.clone(),
                    balance: reserves[i].clone().to_signed_bytes_be(),
                    component_id: pool_address_hex.as_bytes().to_vec(),
                },
            );
        }
    };

    eh.on::<abi::pair::events::Sync, _>(&mut on_sync);
    eh.handle_events();
}

fn merge_block(
    tx_changes: &mut HashMap<Vec<u8>, PartialChanges>,
    block_entity_changes: &mut BlockChanges,
) {
    let mut tx_entity_changes_map = HashMap::new();

    for change in block_entity_changes.changes.clone().into_iter() {
        let transaction = change.tx.as_ref().unwrap();
        tx_entity_changes_map
            .entry(transaction.hash.clone())
            .and_modify(|c: &mut TransactionChanges| {
                c.component_changes.extend(change.component_changes.clone());
                c.entity_changes.extend(change.entity_changes.clone());
            })
            .or_insert(change);
    }

    for change in tx_entity_changes_map.values_mut() {
        let tx = change.clone().tx.expect("Transaction not found").clone();
        if let Some(partial_changes) = tx_changes.remove(&tx.hash) {
            change.entity_changes = partial_changes.clone().consolidate_entity_changes();
            change.balance_changes = partial_changes.balance_changes.into_values().collect();
        }
    }

    for partial_changes in tx_changes.values() {
        tx_entity_changes_map.insert(
            partial_changes.transaction.hash.clone(),
            TransactionChanges {
                tx: Some(partial_changes.transaction.clone()),
                contract_changes: vec![],
                entity_changes: partial_changes.clone().consolidate_entity_changes(),
                balance_changes: partial_changes.balance_changes.clone().into_values().collect(),
                component_changes: vec![],
                ..Default::default()
            },
        );
    }

    block_entity_changes.changes = tx_entity_changes_map.into_values().collect();
}

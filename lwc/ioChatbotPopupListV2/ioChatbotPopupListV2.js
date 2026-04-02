import { LightningElement, api, track } from 'lwc';
import { ShowToastEvent } from 'lightning/platformShowToastEvent';
import { NavigationMixin } from 'lightning/navigation';
import getOrderIngestionLines from '@salesforce/apex/OrderIngestionLineController.getOrderIngestionLines';
import saveInsertionPayload from '@salesforce/apex/OrderIngestionLineController.saveInsertionPayload';
import insertOrderLineItems from '@salesforce/apex/OrderLineItemRetryController.insertOrderLineItems';

const TEST_ORDER_ID = '801dN00000niyVlQAI';

export default class IoChatbotPopupListV2 extends NavigationMixin(LightningElement) {
    _orderId = '';

    @track popupListV2AllItems = [];
    @track v2ActionMenuPosition = { top: 0, left: 0 };
    @track v2SearchTerm = '';
    @track v2SelectedStatus = 'all';
    @track v2ShowFilterDropdown = false;
    @track v2OpenActionMenuId = null;
    @track v2ActionMenuFlipUp = false;
    @track v2ExpandedDetailsIds = [];
    @track v2ErrorDropdownItemId = null;
    @track isDetailsOpen = false;
    @track isEditMode = false;
    @track selectedRecordId = null;
    @track detailsSaving = false;
    @track v2OrderId = '';
    @track v2HeaderStatus = '';
    @track v2CampaignName = '';
    @track v2EffectiveDate = '';
    @track v2EndDate = '';
    @track v2OrderUrl = '';
    @track v2QuoteUrl = '';
    @track v2OpportunityUrl = '';
    @track v2AccountUrl = '';

    originalDetails = {};
    draftDetails = {};

    @api
    get orderId() {
        return this._orderId;
    }

    set orderId(value) {
        const normalized = value || '';
        if (normalized === this._orderId) return;
        this._orderId = normalized;
        this.v2OrderId = normalized;

        if (this.isConnected) {
            this.initializeView();
        }
    }

    connectedCallback() {
        this.initializeView();
    }

    @api
    async refresh() {
        const refreshOrderId = this.v2OrderId || this._orderId;
        if (!refreshOrderId) return;
        await this.loadOrderIngestionData(refreshOrderId, { silentRefresh: true });
    }

    async initializeView() {
        this.resetUiState();

        const targetOrderId = this._orderId || this.v2OrderId || TEST_ORDER_ID;
        if (!targetOrderId) {
            this.popupListV2AllItems = [];
            return;
        }

        this.v2OrderId = targetOrderId;
        await this.loadOrderIngestionData(targetOrderId);
    }

    resetUiState() {
        this.v2SearchTerm = '';
        this.v2SelectedStatus = 'all';
        this.v2ShowFilterDropdown = false;
        this.v2OpenActionMenuId = null;
        this.v2ActionMenuFlipUp = false;
        this.v2ExpandedDetailsIds = [];
        this.v2ErrorDropdownItemId = null;
        this.v2EffectiveDate = '';
        this.v2EndDate = '';
        this.resetDetailsState();
    }

    get filteredPopupListV2Items() {
        let items = this.popupListV2AllItems || [];

        if (this.v2SelectedStatus !== 'all') {
            items = items.filter(item => {
                const status = (item.status || '').toLowerCase();
                return status.includes(this.v2SelectedStatus);
            });
        }

        if (this.v2SearchTerm && this.v2SearchTerm.trim() !== '') {
            const searchLower = this.v2SearchTerm.toLowerCase().trim();
            items = items.filter(item => (item.columns || []).some(col => {
                const val = (col.value || '').toString().toLowerCase();
                return val.includes(searchLower);
            }));
        }

        return items.map(item => {
            const isEditingThisItem = this.isEditMode && this.selectedRecordId === item.id;
            const details = this.formatItemDetails(item).map(d => ({
                ...d,
                inputValue: isEditingThisItem && this.draftDetails ? this.draftDetails[d.key] : d.value
            }));

            return {
                ...item,
                actionItems: this.getActionMenuItems(item),
                isActionMenuOpen: this.v2OpenActionMenuId === item.id,
                actionMenuClass: this.v2OpenActionMenuId === item.id && this.v2ActionMenuFlipUp
                    ? 'action-dropdown-menu-inline flip-up'
                    : 'action-dropdown-menu-inline',
                isDetailsExpanded: this.v2ExpandedDetailsIds.includes(item.id),
                isErrorDropdownOpen: this.v2ErrorDropdownItemId === item.id,
                detailsData: details,
                detailsTitleClass: this.getDetailsTitleClass(item.status),
                detailsContainerClass: this.getDetailsContainerClass(item.status)
            };
        });
    }

    get v2DisplayCount() {
        const filtered = this.filteredPopupListV2Items.length;
        const total = this.popupListV2AllItems.length;
        return `1-${filtered} of ${total}`;
    }

    get v2StatusOptions() {
        const uniqueStatuses = new Set(
            (this.popupListV2AllItems || []).map(i => (i.status || '').toLowerCase()).filter(Boolean)
        );
        const options = [{ label: 'All Statuses', value: 'all', isSelected: this.v2SelectedStatus === 'all' }];
        uniqueStatuses.forEach(status => {
            const label = status ? status.charAt(0).toUpperCase() + status.slice(1) : status;
            options.push({ label, value: status, isSelected: this.v2SelectedStatus === status });
        });
        return options;
    }

    get v2ColumnHeaders() {
        const first = (this.popupListV2AllItems && this.popupListV2AllItems[0]) || {};
        return first.columns || [];
    }

    get v2CurrentFilterLabel() {
        const option = this.v2StatusOptions.find(opt => opt.value === this.v2SelectedStatus);
        return option ? option.label : 'All Statuses';
    }

    get v2OpenActionMenuItems() {
        if (!this.v2OpenActionMenuId) return [];
        const item = this.popupListV2AllItems.find(i => i.id === this.v2OpenActionMenuId);
        if (!item) return [];
        return this.getActionMenuItems(item);
    }

    get v2ActionMenuStyle() {
        return `top: ${this.v2ActionMenuPosition.top}px; left: ${this.v2ActionMenuPosition.left}px;`;
    }

    get v2ActionMenuClass() {
        return this.v2ActionMenuFlipUp
            ? 'action-menu-portal flip-up'
            : 'action-menu-portal';
    }

    getActionMenuItems(item) {
        const statusLower = (item.status || '').toLowerCase();
        const hasError = item.hasError;

        const viewDetails = { key: 'view_details', label: 'View details', icon: 'utility:preview', iconClass: 'action-icon-default' };
        const editLineItem = { key: 'edit_lineitem', label: 'Edit line item', icon: 'utility:edit', iconClass: 'action-icon-default' };
        const retryProcessing = { key: 'retry_processing', label: 'Retry processing', icon: 'utility:refresh', iconClass: 'action-icon-default' };
        const viewError = { key: 'view_error', label: 'View error', icon: 'utility:warning', iconClass: 'action-icon-error' };
        const mapProduct = { key: 'map_product', label: 'Map product', icon: 'utility:link', iconClass: 'action-icon-warning' };

        const actions = [];

        if (statusLower.includes('inserted') || statusLower === 'success') {
            actions.push(viewDetails);
        } else if (statusLower.includes('rejected')) {
            actions.push(viewDetails, editLineItem, retryProcessing);
            if (hasError) {
                actions.push(viewError);
            }
        } else if (statusLower.includes('skipped')) {
            actions.push(viewDetails, editLineItem, retryProcessing);
            if (hasError) {
                actions.push(viewError);
            }
            actions.push(mapProduct);
        } else {
            actions.push(viewDetails);
            if (hasError) {
                actions.push(viewError);
            }
        }
        return actions;
    }

    formatItemDetails(item) {
        if (!item || !item.rawData) {
            return [{ key: 'no-data', label: 'Details', value: 'No additional details available', displayValue: 'No additional details available', inputType: 'text' }];
        }

        const raw = item.rawData;
        const details = [];

        const formatLabel = (key) => {
            if (!key) return '';

            // Custom explicitly mapped labels
            if (key.toLowerCase() === 'quotelineitemid') {
                return 'Media Plan Placement ID';
            }

            let formatted = key.replace(/__c/gi, '').replace(/__r/gi, '');
            formatted = formatted.replace(/_/g, ' ');
            formatted = formatted.replace(/([a-z])([A-Z])/g, '$1 $2');
            return formatted.split(' ').map(word =>
                word.charAt(0).toUpperCase() + word.slice(1)
            ).join(' ').trim();
        };

        const getInputType = (key, value) => {
            const keyLower = key.toLowerCase();
            if (keyLower === 'id' || keyLower.endsWith('id')) return 'text';
            if (keyLower.includes('date')) return 'date';
            if (keyLower.includes('price') || keyLower.includes('amount') || keyLower.includes('cost') || keyLower.includes('budget')) return 'number';
            if (keyLower.includes('quantity') || keyLower.includes('impressions') || keyLower.includes('mille')) return 'number';
            if (typeof value === 'number') return 'number';
            return 'text';
        };

        const formatDisplayValue = (key, value) => {
            if (value === null || value === undefined) return '-';

            const keyLower = key.toLowerCase();
            if (keyLower.includes('price') || keyLower.includes('amount') || keyLower.includes('cost') || keyLower.includes('budget')) {
                if (typeof value === 'number') {
                    return `\u20B9${Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
                }
            }

            if (typeof value === 'number') {
                return Number(value).toLocaleString();
            }

            if (keyLower.includes('date') && typeof value === 'string') {
                return value;
            }

            return String(value);
        };

        const formatInputValue = (key, value) => {
            if (value === null || value === undefined) return '';

            const keyLower = key.toLowerCase();
            if (keyLower.includes('date') && typeof value === 'string') {
                if (value.includes('-')) return value.slice(0, 10);
                const d = new Date(value);
                return isNaN(d.getTime()) ? '' : d.toISOString().slice(0, 10);
            }

            return value;
        };

        const skipKeys = ['attributes', 'url', 'pricebookentryid', 'id'];

        const isSalesforceId = (key, value) => {
            if (typeof value !== 'string') return false;
            const keyLower = key.toLowerCase();
            if (keyLower === 'id' || keyLower.endsWith('id')) {
                return value.length === 15 || value.length === 18;
            }
            return false;
        };

        const processObject = (obj, prefix = '') => {
            if (!obj || typeof obj !== 'object') return;

            Object.entries(obj).forEach(([key, value]) => {
                if (key.toLowerCase() === 'id') return;
                if (skipKeys.includes(key.toLowerCase())) return;

                const fullKey = prefix ? `${prefix}_${key}` : key;

                if (value && typeof value === 'object' && !Array.isArray(value)) {
                    processObject(value, fullKey);
                } else if (!Array.isArray(value)) {
                    const inputType = getInputType(key, value);
                    const isLink = isSalesforceId(key, value);
                    const recordUrl = isLink ? `/lightning/r/${value}/view` : null;
                    details.push({
                        key: fullKey,
                        label: formatLabel(key),
                        value: formatInputValue(key, value),
                        displayValue: formatDisplayValue(key, value),
                        inputType: inputType,
                        step: inputType === 'number' ? '0.01' : undefined,
                        isLink: isLink,
                        recordId: isLink ? value : null,
                        url: recordUrl
                    });
                }
            });
        };

        processObject(raw);

        return details.length > 0 ? details : [{ key: 'no-data', label: 'Details', value: 'No additional details available', displayValue: 'No additional details available', inputType: 'text' }];
    }

    closeV2ActionMenu() {
        this.v2OpenActionMenuId = null;
    }

    closeErrorDropdown(event) {
        event.stopPropagation();
        this.v2ErrorDropdownItemId = null;
    }

    toggleDetailsExpansion(itemId) {
        if (this.v2ExpandedDetailsIds.includes(itemId)) {
            this.closeDetails();
            return;
        }
        this.openDetails(itemId, false);
    }

    collapseDetails(event) {
        event.stopPropagation();
        if (this.isEditMode && this.hasDraftChanges()) {
            if (!confirm('Discard changes?')) {
                return;
            }
        }
        const itemId = event.currentTarget.dataset.itemid;
        this.v2ExpandedDetailsIds = this.v2ExpandedDetailsIds.filter(id => id !== itemId);
        this.resetDetailsState();
    }

    getDetailsTitleClass(status) {
        const s = (status || '').toLowerCase();
        if (s.includes('inserted') || s === 'success') return 'details-title status-success';
        if (s.includes('rejected') || s === 'error') return 'details-title status-error';
        if (s.includes('skipped') || s === 'warning') return 'details-title status-warning';
        return 'details-title';
    }

    getDetailsContainerClass(status) {
        const s = (status || '').toLowerCase();
        if (s.includes('inserted') || s === 'success') return 'details-expand-container status-success';
        if (s.includes('rejected') || s === 'error') return 'details-expand-container status-error';
        if (s.includes('skipped') || s === 'warning') return 'details-expand-container status-warning';
        return 'details-expand-container';
    }

    openDetails(itemId, startEdit = false) {
        this.v2ExpandedDetailsIds = [itemId];
        this.selectedRecordId = itemId;
        this.isDetailsOpen = true;
        this.isEditMode = startEdit;
        this.v2ErrorDropdownItemId = null;
        const item = this.popupListV2AllItems.find(i => i.id === itemId);
        const detailsArray = this.formatItemDetails(item);
        this.originalDetails = this.detailsArrayToObject(detailsArray);
        this.draftDetails = this.deepClone(this.originalDetails);
    }

    closeDetails() {
        this.v2ExpandedDetailsIds = [];
        this.resetDetailsState();
    }

    resetDetailsState() {
        this.isDetailsOpen = false;
        this.isEditMode = false;
        this.selectedRecordId = null;
        this.detailsSaving = false;
        this.originalDetails = {};
        this.draftDetails = {};
    }

    detailsArrayToObject(detailsArray = []) {
        const obj = {};
        detailsArray.forEach(d => {
            obj[d.key] = d.value;
        });
        return obj;
    }

    deepClone(obj) {
        return JSON.parse(JSON.stringify(obj || {}));
    }

    hasDraftChanges() {
        return JSON.stringify(this.draftDetails) !== JSON.stringify(this.originalDetails);
    }

    enterEditMode(event) {
        event.stopPropagation();
        this.isEditMode = true;
    }

    cancelEdit(event) {
        event.stopPropagation();
        this.draftDetails = this.deepClone(this.originalDetails);
        this.isEditMode = false;
    }

    handleDetailInputChange(event) {
        const key = event.target.dataset.key;
        const value = event.target.value;
        this.draftDetails = { ...this.draftDetails, [key]: value };
    }

    validateDraft() {
        const errors = [];

        const getFirstDraftValue = (candidateKeys = []) => {
            const keys = Object.keys(this.draftDetails || {});
            for (const candidate of candidateKeys) {
                const candidateLower = candidate.toLowerCase();
                const keyMatch = keys.find(k => k.toLowerCase() === candidateLower || k.toLowerCase().endsWith(`_${candidateLower}`));
                if (keyMatch && this.draftDetails[keyMatch] !== undefined && this.draftDetails[keyMatch] !== null && this.draftDetails[keyMatch] !== '') {
                    return this.draftDetails[keyMatch];
                }
            }
            return null;
        };

        const start = getFirstDraftValue(['AdRequestedStartDate', 'requestedStartDate', 'ServiceDate', 'startDate', 'start_date']);
        const end = getFirstDraftValue(['AdRequestedEndDate', 'requestedEndDate', 'EndDate', 'endDate', 'end_date']);
        if (start && end && new Date(start) > new Date(end)) {
            errors.push('Start date must be before or equal to end date');
        }

        Object.keys(this.draftDetails || {}).forEach(key => {
            const value = this.draftDetails[key];
            const keyLower = key.toLowerCase();
            const isIdField = keyLower === 'id' || keyLower.endsWith('id');
            const shouldBeNumber = !isIdField && (
                keyLower.includes('price') ||
                keyLower.includes('amount') ||
                keyLower.includes('cost') ||
                keyLower.includes('budget') ||
                keyLower.includes('quantity') ||
                keyLower.includes('impressions') ||
                keyLower.includes('mille')
            );
            if (shouldBeNumber && value !== undefined && value !== null && value !== '' && isNaN(Number(value))) {
                errors.push(`${key} must be a number`);
            }
        });

        return errors;
    }

    async handleSaveDetails(event) {
        event.stopPropagation();
        const errors = this.validateDraft();
        if (errors.length) {
            this.showToast('Validation', errors.join('; '), 'error');
            return;
        }

        this.detailsSaving = true;
        try {
            const currentItem = this.popupListV2AllItems.find(i => i.id === this.selectedRecordId);
            if (!currentItem) {
                throw new Error('Unable to find the selected line item.');
            }

            const targetRecordId = currentItem.ingestionLineId || currentItem.id;
            if (!targetRecordId) {
                throw new Error('Missing Order Ingestion Line record id.');
            }

            const updatedRawData = this.applyDraftToRawData(currentItem.rawData || {}, this.draftDetails);
            const payloadToPersist = JSON.stringify(updatedRawData);

            await saveInsertionPayload({
                orderIngestionLineId: targetRecordId,
                insertionPayloadJson: payloadToPersist
            });

            this.originalDetails = this.deepClone(this.draftDetails);
            this.isEditMode = false;
            this.refreshRowDetails(updatedRawData);
            this.showToast('Success', 'Line item updated', 'success');
        } catch (err) {
            this.showToast('Error', this.getApexErrorMessage(err) || 'Failed to save', 'error');
        } finally {
            this.detailsSaving = false;
        }
    }

    applyDraftToRawData(rawData = {}, draft = {}) {
        const cloned = this.deepClone(rawData || {});
        const skipKeys = new Set(['attributes', 'url']);

        const coerceDraftValue = (key, draftValue, existingValue) => {
            if (draftValue === null || draftValue === undefined) return existingValue;
            if (draftValue === '') return '';

            const keyLower = (key || '').toLowerCase();
            const looksNumericByName = (
                keyLower.includes('price') ||
                keyLower.includes('amount') ||
                keyLower.includes('cost') ||
                keyLower.includes('budget') ||
                keyLower.includes('quantity') ||
                keyLower.includes('impressions') ||
                keyLower.includes('mille')
            ) && !(keyLower === 'id' || keyLower.endsWith('id'));

            if (typeof existingValue === 'number') {
                const n = Number(draftValue);
                return isNaN(n) ? existingValue : n;
            }
            if (typeof existingValue === 'boolean') {
                if (typeof draftValue === 'boolean') return draftValue;
                const normalized = String(draftValue).toLowerCase();
                if (normalized === 'true') return true;
                if (normalized === 'false') return false;
                return existingValue;
            }
            if (keyLower.includes('date')) {
                return String(draftValue).slice(0, 10);
            }
            if ((existingValue === null || existingValue === undefined) && looksNumericByName) {
                const n = Number(draftValue);
                return isNaN(n) ? draftValue : n;
            }

            return draftValue;
        };

        const applyRecursive = (obj, prefix = '') => {
            if (!obj || typeof obj !== 'object') return;
            Object.keys(obj).forEach(key => {
                if (skipKeys.has(String(key).toLowerCase())) return;
                const value = obj[key];
                const fullKey = prefix ? `${prefix}_${key}` : key;

                if (value && typeof value === 'object' && !Array.isArray(value)) {
                    applyRecursive(value, fullKey);
                    return;
                }

                if (Array.isArray(value)) return;
                if (Object.prototype.hasOwnProperty.call(draft, fullKey)) {
                    obj[key] = coerceDraftValue(key, draft[fullKey], value);
                }
            });
        };

        applyRecursive(cloned);
        return cloned;
    }

    refreshRowDetails(updatedRawData) {
        if (!this.selectedRecordId) return;

        this.popupListV2AllItems = this.popupListV2AllItems.map(item => {
            if (item.id !== this.selectedRecordId) return item;
            const updated = { ...item, rawData: updatedRawData };

            const firstValueByKeys = (sources = [], keys = []) => {
                for (const source of sources) {
                    if (!source || typeof source !== 'object') continue;
                    for (const key of keys) {
                        if (source[key] !== undefined && source[key] !== null && source[key] !== '') {
                            return source[key];
                        }
                    }
                }
                return null;
            };

            const toDisplayDate = (rawDate, fallback) => {
                if (!rawDate) return fallback;
                const d = new Date(rawDate);
                return isNaN(d.getTime())
                    ? String(rawDate)
                    : d.toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' });
            };

            const toDisplayCurrency = (rawValue, fallback) => {
                if (rawValue === null || rawValue === undefined || rawValue === '') return fallback;
                const n = Number(rawValue);
                if (isNaN(n)) return String(rawValue);
                return `\u20B9${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
            };

            const sources = [updatedRawData, updatedRawData && updatedRawData.QuoteLineItem];
            const productValue = firstValueByKeys(sources, ['Name', 'name', 'Description', 'description']);
            const startDateValue = firstValueByKeys(sources, ['AdRequestedStartDate', 'requestedStartDate', 'ServiceDate', 'start_date', 'startDate']);
            const endDateValue = firstValueByKeys(sources, ['AdRequestedEndDate', 'requestedEndDate', 'EndDate', 'end_date', 'endDate']);
            const totalPriceValue = firstValueByKeys(sources, ['TotalPrice', 'totalPrice', 'budget']);
            const unitPriceValue = firstValueByKeys(sources, ['UnitPrice', 'unitPrice', 'ListPrice', 'listPrice']);
            const quantityValue = firstValueByKeys(sources, ['Quantity', 'quantity']);
            const calculatedTotal = (totalPriceValue !== null && totalPriceValue !== undefined)
                ? totalPriceValue
                : (unitPriceValue !== null && unitPriceValue !== undefined && quantityValue !== null && quantityValue !== undefined)
                    ? Number(unitPriceValue) * Number(quantityValue)
                    : null;

            updated.columns = (updated.columns || []).map(col => {
                const keyLower = (col.key || '').toLowerCase();
                if (keyLower === 'product') {
                    return { ...col, value: productValue || col.value };
                }
                if (keyLower.includes('start')) {
                    return { ...col, value: toDisplayDate(startDateValue, col.value) };
                }
                if (keyLower.includes('end')) {
                    return { ...col, value: toDisplayDate(endDateValue, col.value) };
                }
                if (keyLower.includes('net') || keyLower.includes('amount')) {
                    return { ...col, value: toDisplayCurrency(calculatedTotal, col.value) };
                }
                return col;
            });

            updated.displayData = (updated.columns || []).reduce((acc, col) => {
                acc[col.key] = col.value;
                return acc;
            }, {});

            updated.detailsData = this.formatItemDetails(updated);
            return updated;
        });
    }

    async loadOrderIngestionData(orderId, options = {}) {
        try {
            const resolvedOrderId = orderId || TEST_ORDER_ID;
            const isSilentRefresh = options && options.silentRefresh === true;

            const records = await getOrderIngestionLines({ orderId: resolvedOrderId });

            if (!records || records.length === 0) {
                this.popupListV2AllItems = [];
                this.v2OrderId = resolvedOrderId;
                this.v2HeaderStatus = '';
                this.v2CampaignName = `Order ${resolvedOrderId}`;
                this.v2EffectiveDate = '';
                this.v2EndDate = '';
                this.v2OrderUrl = await this.generateRecordUrl(resolvedOrderId);
                this.v2QuoteUrl = '';
                this.v2OpportunityUrl = '';
                this.v2AccountUrl = '';
                if (!isSilentRefresh) {
                    this.showToast('Info', 'No Order Ingestion Lines found for this order.', 'info');
                }
                return;
            }

            const msgId = Date.now();
            const first = records[0] || {};
            const orderIdFromSoql = first.Order__c || resolvedOrderId;
            const headerMeta = this.extractOrderHeaderMeta(first.Order_details__c);
            const headerStatus = headerMeta.status || first.Processing_Status__c || '';
            const campaignName = headerMeta.campaignName || `Order ${orderIdFromSoql}`;
            const effectiveDate = this.formatHeaderDate(headerMeta.effectiveDate);
            const endDate = this.formatHeaderDate(headerMeta.endDate);

            const [orderUrl, quoteUrl, opportunityUrl, accountUrl] = await Promise.all([
                this.generateRecordUrl(orderIdFromSoql),
                this.generateRecordUrl(headerMeta.quoteId),
                this.generateRecordUrl(headerMeta.opportunityId),
                this.generateRecordUrl(headerMeta.accountId)
            ]);

            const baseRows = records.map((record, index) => {
                const statusLabel = record.Processing_Status__c || 'unknown';
                const status = statusLabel.toLowerCase();

                let rowClass = '';
                let statusIcon = '';
                if (status.includes('inserted') || status === 'success') {
                    rowClass = 'row-inserted';
                    statusIcon = 'utility:success';
                } else if (status.includes('rejected') || status === 'error') {
                    rowClass = 'row-rejected';
                    statusIcon = 'utility:error';
                } else if (status.includes('skipped') || status === 'warning') {
                    rowClass = 'row-skipped';
                    statusIcon = 'utility:warning';
                } else {
                    rowClass = 'row-default';
                    statusIcon = 'utility:info';
                }

                const extracted = this.parseExtractedIoDetails(record.Extracted_io_details__c);
                const displayData = { name: String(index + 1), ...extracted };

                const parsedDetails = this.parseInsertionPayload(record.Insertion_Payload_JSON_c__c);
                let detailCandidate = Array.isArray(parsedDetails) && parsedDetails.length ? parsedDetails[0] : parsedDetails;
                while (Array.isArray(detailCandidate) && detailCandidate.length > 0) {
                    detailCandidate = detailCandidate[0];
                }

                const rawData = (detailCandidate && typeof detailCandidate === 'object' && !Array.isArray(detailCandidate))
                    ? detailCandidate
                    : { rawPayload: record.Insertion_Payload_JSON_c__c };

                const errorParsed = this.parseApiResponse(record.API_Response_JSON_c__c);
                const hasError = Array.isArray(errorParsed.errors) && errorParsed.errors.length > 0;

                return {
                    id: record.Id || `oil-v2-${msgId}-${record.Name || index}`,
                    ingestionLineId: record.Id || null,
                    status,
                    statusLabel,
                    statusIcon,
                    rowClass,
                    displayData,
                    rawData,
                    hasError,
                    errorData: errorParsed.errors || [],
                    errorMessage: errorParsed.errorMessage || ''
                };
            });

            const coreKeys = ['name', 'product', 'startDate', 'endDate', 'netAmount'];
            const optionalKeysInOrder = [];
            const seenOptional = new Set(coreKeys);
            baseRows.forEach(r => {
                Object.keys(r.displayData || {}).forEach(k => {
                    if (!seenOptional.has(k) && !optionalKeysInOrder.includes(k)) {
                        optionalKeysInOrder.push(k);
                    }
                });
            });

            const includedOptionalKeys = optionalKeysInOrder.filter(k =>
                baseRows.some(r => {
                    const v = r.displayData ? r.displayData[k] : undefined;
                    return v !== null && v !== undefined && String(v).trim() !== '';
                })
            );

            const columnKeys = [...coreKeys, ...includedOptionalKeys];

            const normalizedRecords = baseRows.map(r => ({
                ...r,
                columns: columnKeys.map(key => ({
                    key,
                    value: (r.displayData && r.displayData[key] !== undefined && r.displayData[key] !== null)
                        ? r.displayData[key]
                        : ''
                }))
            }));

            this.popupListV2AllItems = normalizedRecords;
            this.v2OrderId = orderIdFromSoql;
            this.v2HeaderStatus = headerStatus;
            this.v2CampaignName = campaignName;
            this.v2EffectiveDate = effectiveDate;
            this.v2EndDate = endDate;
            this.v2OrderUrl = orderUrl;
            this.v2QuoteUrl = quoteUrl;
            this.v2OpportunityUrl = opportunityUrl;
            this.v2AccountUrl = accountUrl;

            if (isSilentRefresh) {
                this.v2OpenActionMenuId = null;
                this.v2ErrorDropdownItemId = null;
                this.v2ExpandedDetailsIds = [];
                this.resetDetailsState();
            }
        } catch (error) {
            if (!options || options.silentRefresh !== true) {
                this.showToast('Error', 'Failed to load Order Ingestion Lines: ' + (error.body?.message || error.message), 'error');
            }
        }
    }

    parseExtractedIoDetails(extractedDetails) {
        const displayData = {
            product: '',
            startDate: '',
            endDate: '',
            netAmount: ''
        };

        if (!extractedDetails) {
            return displayData;
        }

        try {
            const nameMatch = extractedDetails.match(/name=['"](.*?)['"]/);
            const startMatch = extractedDetails.match(/start_date=['"](.*?)['"]/);
            const endMatch = extractedDetails.match(/end_date=['"](.*?)['"]/);
            const budgetMatch = extractedDetails.match(/budget=([\d.]+)/);
            const objectiveMatch = extractedDetails.match(/objective=(?:'([^']*)'|"([^"]*)"|([^\s]+))/);

            if (nameMatch) displayData.product = nameMatch[1];
            if (startMatch) {
                const date = new Date(startMatch[1]);
                displayData.startDate = date.toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' });
            }
            if (endMatch) {
                const date = new Date(endMatch[1]);
                displayData.endDate = date.toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric' });
            }
            if (budgetMatch) {
                const budget = parseFloat(budgetMatch[1]);
                displayData.netAmount = '\u20B9' + budget.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            }
            if (objectiveMatch) {
                const objVal = (objectiveMatch[1] || objectiveMatch[2] || objectiveMatch[3] || '').trim();
                if (objVal && objVal !== 'None' && objVal !== 'null') {
                    displayData.objective = objVal;
                }
            }
        } catch (e) {
            // no-op: leave displayData defaults
        }

        return displayData;
    }

    replacePythonKeywordsOutsideStrings(input = '') {
        let output = '';
        let token = '';
        let inString = false;
        let escapeNext = false;

        const flushToken = () => {
            if (!token) return;
            if (token === 'None') output += 'null';
            else if (token === 'True') output += 'true';
            else if (token === 'False') output += 'false';
            else output += token;
            token = '';
        };

        for (let i = 0; i < input.length; i++) {
            const ch = input[i];

            if (inString) {
                output += ch;
                if (escapeNext) {
                    escapeNext = false;
                    continue;
                }
                if (ch === '\\') {
                    escapeNext = true;
                    continue;
                }
                if (ch === '"') {
                    inString = false;
                }
                continue;
            }

            if (/[A-Za-z0-9_]/.test(ch)) {
                token += ch;
                continue;
            }

            flushToken();
            if (ch === '"') {
                inString = true;
            }
            output += ch;
        }

        flushToken();
        return output;
    }

    normalizePythonLikeJson(rawValue) {
        if (rawValue === null || rawValue === undefined) return '';
        const input = String(rawValue).trim();
        if (!input) return '';

        let withJsonStrings = '';
        let inSingle = false;
        let inDouble = false;
        let escapeNext = false;

        for (let i = 0; i < input.length; i++) {
            const ch = input[i];

            if (inSingle) {
                if (escapeNext) {
                    if (ch === "'") {
                        withJsonStrings += "'";
                    } else if (ch === '"') {
                        withJsonStrings += '\\"';
                    } else if (ch === '\\') {
                        withJsonStrings += '\\\\';
                    } else if (['n', 'r', 't', 'b', 'f', 'u', '/'].includes(ch)) {
                        withJsonStrings += `\\${ch}`;
                    } else {
                        withJsonStrings += ch;
                    }
                    escapeNext = false;
                    continue;
                }

                if (ch === '\\') {
                    escapeNext = true;
                    continue;
                }
                if (ch === "'") {
                    inSingle = false;
                    withJsonStrings += '"';
                    continue;
                }
                if (ch === '"') {
                    withJsonStrings += '\\"';
                    continue;
                }

                withJsonStrings += ch;
                continue;
            }

            if (inDouble) {
                withJsonStrings += ch;
                if (escapeNext) {
                    escapeNext = false;
                    continue;
                }
                if (ch === '\\') {
                    escapeNext = true;
                    continue;
                }
                if (ch === '"') {
                    inDouble = false;
                }
                continue;
            }

            if (ch === "'") {
                inSingle = true;
                withJsonStrings += '"';
                continue;
            }
            if (ch === '"') {
                inDouble = true;
            }

            withJsonStrings += ch;
        }

        return this.replacePythonKeywordsOutsideStrings(withJsonStrings);
    }

    parsePythonLikeJson(rawValue) {
        const normalized = this.normalizePythonLikeJson(rawValue);
        if (!normalized) return null;

        try {
            return JSON.parse(normalized);
        } catch (e) {
            return null;
        }
    }

    parseInsertionPayload(insertionPayload) {
        if (!insertionPayload) {
            return [];
        }
        if (typeof insertionPayload === 'object') {
            return insertionPayload;
        }

        try {
            const direct = JSON.parse(insertionPayload);
            return direct;
        } catch (e) {
            const parsed = this.parsePythonLikeJson(insertionPayload);
            if (parsed !== null) {
                return parsed;
            }

            return [{ rawPayload: insertionPayload }];
        }
    }

    parseApiResponse(apiResponse) {
        const result = {
            success: true,
            errors: [],
            errorMessage: ''
        };

        if (!apiResponse || apiResponse === 'None') {
            return result;
        }
        if (typeof apiResponse === 'object') {
            const parsed = apiResponse;
            result.success = parsed.success === true;
            if (parsed.errors && Array.isArray(parsed.errors) && parsed.errors.length > 0) {
                result.errors = parsed.errors.map((err, idx) => ({
                    id: `err-${idx}`,
                    statusCode: err.statusCode || 'ERROR',
                    message: err.message || 'Unknown error',
                    fields: Array.isArray(err.fields) ? err.fields.join(', ') : (err.fields || '')
                }));
                result.errorMessage = result.errors.map(e => e.message).join('; ');
            }
            return result;
        }

        try {
            let parsed;
            try {
                parsed = JSON.parse(apiResponse);
            } catch (e) {
                parsed = this.parsePythonLikeJson(apiResponse);
            }

            if (!parsed || typeof parsed !== 'object') {
                throw new Error('Unable to parse API response payload');
            }

            result.success = parsed.success === true;

            if (parsed.errors && Array.isArray(parsed.errors) && parsed.errors.length > 0) {
                result.errors = parsed.errors.map((err, idx) => ({
                    id: `err-${idx}`,
                    statusCode: err.statusCode || 'ERROR',
                    message: err.message || 'Unknown error',
                    fields: Array.isArray(err.fields) ? err.fields.join(', ') : (err.fields || '')
                }));
                result.errorMessage = result.errors.map(e => e.message).join('; ');
            }
        } catch (e) {
            result.success = false;
            result.errors = [{ id: 'err-0', statusCode: 'PARSE_ERROR', message: apiResponse, fields: '' }];
            result.errorMessage = apiResponse;
        }

        return result;
    }

    handleV2SearchInput(event) {
        this.v2SearchTerm = event.target.value;
    }

    toggleV2FilterDropdown() {
        this.v2ShowFilterDropdown = !this.v2ShowFilterDropdown;
    }

    handleV2FilterSelect(event) {
        const selectedValue = event.currentTarget.dataset.value;
        this.v2SelectedStatus = selectedValue;
        this.v2ShowFilterDropdown = false;
    }

    handleV2FilterBlur() {
        setTimeout(() => {
            this.v2ShowFilterDropdown = false;
        }, 200);
    }

    toggleV2ActionMenu(event) {
        event.stopPropagation();
        const itemId = event.currentTarget.dataset.itemid;
        if (this.v2OpenActionMenuId === itemId) {
            this.v2OpenActionMenuId = null;
            this.v2ActionMenuFlipUp = false;
        } else {
            const button = event.currentTarget;
            const rect = button.getBoundingClientRect();
            const menuHeight = 280;
            const menuWidth = 160;
            const viewportHeight = window.innerHeight;

            const spaceBelow = viewportHeight - rect.bottom;
            this.v2ActionMenuFlipUp = spaceBelow < menuHeight;

            let top;
            let left;
            if (this.v2ActionMenuFlipUp) {
                top = rect.top - 4;
            } else {
                top = rect.bottom + 4;
            }

            left = rect.right - menuWidth;

            if (left < 20) {
                left = 20;
            }

            this.v2ActionMenuPosition = { top, left };
            this.v2OpenActionMenuId = itemId;
        }
    }

    handleV2ActionMenuBlur() {
        setTimeout(() => {
            this.v2OpenActionMenuId = null;
        }, 200);
    }

    handleV2ActionClick(event) {
        event.stopPropagation();
        const actionKey = event.currentTarget.dataset.action;
        const itemId = event.currentTarget.dataset.itemid;

        this.v2OpenActionMenuId = null;

        const item = this.popupListV2AllItems.find(i => i.id === itemId);
        if (!item) return;

        switch (actionKey) {
            case 'view_details':
                this.toggleDetailsExpansion(itemId);
                break;
            case 'view_salesforce':
                // Placeholder for row-specific navigation behavior.
                break;
            case 'edit_lineitem':
                this.openDetails(itemId, true);
                break;
            case 'retry_processing':
                this.retryV2Processing(item);
                break;
            case 'view_error':
                if (this.v2ErrorDropdownItemId === itemId) {
                    this.v2ErrorDropdownItemId = null;
                } else {
                    this.v2ErrorDropdownItemId = itemId;
                }
                break;
            case 'map_product':
                // Placeholder for product mapping behavior.
                break;
            case 'download_json':
                this.downloadItemAsJson(item);
                break;
            default:
                break;
        }
    }

    downloadItemAsJson(item) {
        const dataStr = JSON.stringify(item.rawData || item, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(dataBlob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `lineitem_${item.id}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    async retryV2Processing(item) {
        if (!item) return;
        if (!this.v2OrderId) {
            this.showToast('Retry Failed', 'Order Id is missing for this line item.', 'error');
            return;
        }

        try {
            const lineItemsJson = [JSON.stringify(item.rawData || {})];
            const ingestionLineIds = [item.ingestionLineId || item.id];
            const response = await insertOrderLineItems({
                orderId: this.v2OrderId,
                lineItemsJson: lineItemsJson,
                ingestionLineIds: ingestionLineIds
            });

            const successCount = (response && response.successCount) || 0;
            const failureCount = (response && response.failureCount) || 0;

            if (successCount > 0 && failureCount === 0) {
                this.showToast('Success', 'Line item retried successfully.', 'success');
                return;
            }

            if (successCount > 0 && failureCount > 0) {
                const failureMsg = this.getRetryFailureMessage(response);
                this.showToast('Partial Success', `Inserted ${successCount}. ${failureMsg}`, 'warning');
                return;
            }

            const failureMsg = this.getRetryFailureMessage(response);
            this.showToast('Retry Failed', failureMsg, 'error');
        } catch (error) {
            this.showToast('Retry Failed', this.getApexErrorMessage(error), 'error');
        } finally {
            if (this.v2OrderId) {
                await this.loadOrderIngestionData(this.v2OrderId, { silentRefresh: true });
            }
        }
    }

    getRetryFailureMessage(response) {
        const defaultMsg = 'Failed to insert Order Line Item.';
        if (!response || !Array.isArray(response.results)) return defaultMsg;
        const firstFailure = response.results.find(r => r && r.success === false);
        return firstFailure && firstFailure.message ? firstFailure.message : defaultMsg;
    }

    getApexErrorMessage(error) {
        if (error && error.body && error.body.message) return error.body.message;
        if (error && error.message) return error.message;
        return 'Unexpected error while retrying line item.';
    }

    async generateRecordUrl(recordId) {
        if (!recordId || typeof recordId !== 'string') return '';
        if (!(recordId.length === 15 || recordId.length === 18)) return '';
        try {
            return await this[NavigationMixin.GenerateUrl]({
                type: 'standard__recordPage',
                attributes: { recordId: recordId, actionName: 'view' }
            });
        } catch (e) {
            return '';
        }
    }

    formatHeaderDate(rawValue) {
        if (!rawValue) return '';
        const value = String(rawValue).trim();
        if (!value) return '';

        const parsed = new Date(value);
        if (isNaN(parsed.getTime())) return value;
        return parsed.toLocaleDateString('en-US', {
            month: 'short',
            day: '2-digit',
            year: 'numeric'
        });
    }

    extractOrderHeaderMeta(orderDetailsRaw) {
        const meta = {
            status: '',
            campaignName: '',
            effectiveDate: '',
            endDate: '',
            quoteId: '',
            opportunityId: '',
            accountId: ''
        };
        if (!orderDetailsRaw) return meta;

        const raw = typeof orderDetailsRaw === 'string' ? orderDetailsRaw : String(orderDetailsRaw);

        const findFirstByKeys = (obj, keys, depth = 0) => {
            if (!obj || depth > 5) return '';
            if (typeof obj !== 'object') return '';

            for (const k of Object.keys(obj)) {
                const kLower = k.toLowerCase();
                const wanted = keys.find(w => w.toLowerCase() === kLower);
                if (wanted) {
                    const v = obj[k];
                    if (typeof v === 'string' && v.trim() !== '') return v.trim();
                }
            }

            for (const v of Object.values(obj)) {
                if (v && typeof v === 'object') {
                    if (Array.isArray(v)) {
                        for (const el of v) {
                            const out = findFirstByKeys(el, keys, depth + 1);
                            if (out) return out;
                        }
                    } else {
                        const out = findFirstByKeys(v, keys, depth + 1);
                        if (out) return out;
                    }
                }
            }
            return '';
        };

        const tryParseLooseJson = (s) => {
            const trimmed = s.trim();
            if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) return null;
            return this.parsePythonLikeJson(trimmed);
        };

        try {
            const parsed = JSON.parse(raw);
            if (parsed && typeof parsed === 'object') {
                meta.status = findFirstByKeys(parsed, ['Status', 'status']);
                meta.campaignName = findFirstByKeys(parsed, ['Name', 'name', 'CampaignName', 'campaignName', 'campaign_name']);
                meta.effectiveDate = findFirstByKeys(parsed, ['EffectiveDate', 'effectiveDate', 'effective_date', 'StartDate', 'startDate', 'start_date']);
                meta.endDate = findFirstByKeys(parsed, ['EndDate', 'endDate', 'end_date', 'ExpirationDate', 'expirationDate']);
                meta.quoteId = findFirstByKeys(parsed, ['QuoteId', 'quoteId', 'quote_id', 'Quote__c']);
                meta.opportunityId = findFirstByKeys(parsed, ['OpportunityId', 'opportunityId', 'opportunity_id', 'Opportunity__c']);
                meta.accountId = findFirstByKeys(parsed, ['AccountId', 'accountId', 'account_id', 'Account__c']);
                return meta;
            }
        } catch (e) {
            // continue
        }

        const loose = tryParseLooseJson(raw);
        if (loose && typeof loose === 'object') {
            meta.status = findFirstByKeys(loose, ['Status', 'status']);
            meta.campaignName = findFirstByKeys(loose, ['Name', 'name', 'CampaignName', 'campaignName', 'campaign_name']);
            meta.effectiveDate = findFirstByKeys(loose, ['EffectiveDate', 'effectiveDate', 'effective_date', 'StartDate', 'startDate', 'start_date']);
            meta.endDate = findFirstByKeys(loose, ['EndDate', 'endDate', 'end_date', 'ExpirationDate', 'expirationDate']);
            meta.quoteId = findFirstByKeys(loose, ['QuoteId', 'quoteId', 'quote_id', 'Quote__c']);
            meta.opportunityId = findFirstByKeys(loose, ['OpportunityId', 'opportunityId', 'opportunity_id', 'Opportunity__c']);
            meta.accountId = findFirstByKeys(loose, ['AccountId', 'accountId', 'account_id', 'Account__c']);
            return meta;
        }

        const pickStr = (keyAlternation) => {
            const re = new RegExp(`(?:'|")?(?:${keyAlternation})(?:'|")?\\s*[:=]\\s*(?:'|")([^'"]+)(?:'|")`);
            const m = raw.match(re);
            return m ? m[1].trim() : '';
        };
        const pickId = (keyAlternation) => {
            const re = new RegExp(`(?:'|")?(?:${keyAlternation})(?:'|")?\\s*[:=]\\s*(?:'|")?([a-zA-Z0-9]{15,18})(?:'|")?`);
            const m = raw.match(re);
            return m ? m[1].trim() : '';
        };
        const pickDate = (keyAlternation) => {
            const quoted = new RegExp(`(?:'|")?(?:${keyAlternation})(?:'|")?\\s*[:=]\\s*(?:'|")([^'"]+)(?:'|")`);
            const quotedMatch = raw.match(quoted);
            if (quotedMatch) return quotedMatch[1].trim();

            const iso = new RegExp(`(?:'|")?(?:${keyAlternation})(?:'|")?\\s*[:=]\\s*([0-9]{4}-[0-9]{2}-[0-9]{2})`);
            const isoMatch = raw.match(iso);
            return isoMatch ? isoMatch[1].trim() : '';
        };

        meta.status = pickStr('Status|status');
        meta.campaignName = pickStr('Name|name|CampaignName|campaignName|campaign_name');
        meta.effectiveDate = pickDate('EffectiveDate|effectiveDate|effective_date|StartDate|startDate|start_date');
        meta.endDate = pickDate('EndDate|endDate|end_date|ExpirationDate|expirationDate');
        meta.quoteId = pickId('QuoteId|quoteId|quote_id|Quote__c');
        meta.opportunityId = pickId('OpportunityId|opportunityId|opportunity_id|Opportunity__c');
        meta.accountId = pickId('AccountId|accountId|account_id|Account__c');
        return meta;
    }

    handleClose() {
        this.dispatchEvent(new CustomEvent('closepopup'));
    }

    showToast(title, message, variant) {
        this.dispatchEvent(new ShowToastEvent({ title, message, variant }));
    }
}
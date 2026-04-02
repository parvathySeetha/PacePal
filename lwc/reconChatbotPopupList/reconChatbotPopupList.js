import { LightningElement, api, track } from 'lwc';

export default class ReconChatbotPopupList extends LightningElement {
    _reconData;

    @api
    get reconData() {
        return this._reconData;
    }
    set reconData(value) {
        this._reconData = value;
        if (value && value.lineItems) {
            this.processData();
        } else {
            this.displayItems = [];
        }
    }

    @track displayItems = [];
    @track openActionMenuId = null;

    actionMenuFlipUp = false;
    actionMenuPosition = { top: 0, left: 0 };

    connectedCallback() {
        if (this.reconData && this.reconData.lineItems) {
            this.processData();
        }
    }

    formatDate(dateStr) {
        if (!dateStr) return 'N/A';

        try {
            const dt = new Date(dateStr);
            if (!isNaN(dt.getTime())) {
                const year = dt.getFullYear();
                const month = String(dt.getMonth() + 1).padStart(2, '0');
                const day = String(dt.getDate()).padStart(2, '0');
                return `${year}-${month}-${day}`;
            }
        } catch (e) {
            // fallback below
        }

        const parts = String(dateStr).split('-');
        if (parts.length === 3) {
            return `${parts[0]}-${parts[1]}-${parts[2]}`;
        }

        return dateStr;
    }

    formatCurrency(value) {
        let num = Number(value || 0);

        if (Object.is(num, -0) || Math.abs(num) < 0.005) {
            num = 0;
        }

        return `₹${num.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        })}`;
    }

    formatNumber(value, min = 0, max = 0) {
        return Number(value || 0).toLocaleString(undefined, {
            minimumFractionDigits: min,
            maximumFractionDigits: max
        });
    }

    processData() {
        this.displayItems = (this.reconData?.lineItems || []).map((item, index) => {
            let statusIcon = 'utility:success';
            let statusLabel = 'Safe';
            let rowClass = 'row-inserted';
            let titleClass = 'details-title status-success';
            let containerClass = 'details-expand-container status-success';

            if (item.description && item.description.includes('Underbilled')) {
                statusIcon = 'utility:warning';
                statusLabel = 'Underbilled';
                rowClass = 'row-skipped';
                titleClass = 'details-title status-warning';
                containerClass = 'details-expand-container status-warning';
            } else if (item.description && item.description.includes('Overbilled')) {
                statusIcon = 'utility:error';
                statusLabel = 'Overbilled';
                rowClass = 'row-rejected';
                titleClass = 'details-title status-error';
                containerClass = 'details-expand-container status-error';
            }

            return {
                ...item,
                id: `oli-${index}`,
                isDetailsExpanded: false,
                statusIcon,
                statusLabel,
                rowClass,
                detailsTitleClass: titleClass,
                detailsContainerClass: containerClass,
                displayRate: this.formatCurrency(item.rate || 0),
                displayRevenue: this.formatCurrency(item.revenue || 0),
                displayBilledRevenue: this.formatCurrency(item.billedRevenue || 0),
                displayImpressions: this.formatNumber(item.billedImpressions || 0, 3, 3),
                displayValidImpressions: this.formatNumber(item.validImpressions || 0, 3, 3),
                displayBilledImpressionsItem: this.formatNumber(item.billedImpressions || 0, 3, 3),
                displayDates: item.periodStart && item.periodEnd
                    ? `${this.formatDate(item.periodStart)} to ${this.formatDate(item.periodEnd)}`
                    : item.dates,
                formattedDailyBlocks: (item.dailyBlocks || []).map((db, i) => {
                    return {
                        ...db,
                        id: `db-${index}-${i}`,
                        displayDate: this.formatDate(db.date),
                        displayOrderLineItem: db.orderLineItem || 'N/A',
                        displayQuoteLineItem: db.quoteLineNumber || 'N/A',
                        displayPricingModel: db.pricingModel || item.pricingModel || 'N/A',
                        displayRate: this.formatCurrency(db.rate || item.rate || 0),
                        displayGross: this.formatNumber(db.gross || 0),
                        displayIvt: `${Number(db.ivtPct || 0).toFixed(2)}%`,
                        displayViewability: `${Number(db.viewabilityPct || 0).toFixed(2)}%`,
                        displayBillableImp: this.formatNumber(db.viewableImpressions || 0, 3, 3),
                        displayDailyRevenue: this.formatCurrency(db.revenue || 0)
                    };
                })
            };
        });
    }

    handleClose() {
        this.dispatchEvent(new CustomEvent('close'));
    }

    toggleDetails(event) {
        const itemId = event.currentTarget.dataset.itemid;
        this.toggleDetailsExpansion(itemId);
    }

    toggleDetailsExpansion(itemId) {
        this.displayItems = this.displayItems.map(item => {
            if (item.id === itemId) {
                return { ...item, isDetailsExpanded: !item.isDetailsExpanded };
            }
            return item;
        });
    }

    collapseDetails(event) {
        const itemId = event.currentTarget.dataset.itemid;
        this.displayItems = this.displayItems.map(item => {
            if (item.id === itemId) {
                return { ...item, isDetailsExpanded: false };
            }
            return item;
        });
    }

    toggleActionMenu(event) {
        event.stopPropagation();

        const itemId = event.currentTarget.dataset.itemid;
        if (this.openActionMenuId === itemId) {
            this.openActionMenuId = null;
            this.actionMenuFlipUp = false;
            return;
        }

        const button = event.currentTarget;
        const rect = button.getBoundingClientRect();
        const menuHeight = 140;
        const menuWidth = 180;
        const viewportHeight = window.innerHeight;

        const spaceBelow = viewportHeight - rect.bottom;
        this.actionMenuFlipUp = spaceBelow < menuHeight;

        let top;
        let left;

        if (this.actionMenuFlipUp) {
            top = rect.top - 4;
        } else {
            top = rect.bottom + 4;
        }

        left = rect.right - menuWidth;
        if (left < 20) {
            left = 20;
        }

        this.actionMenuPosition = { top, left };
        this.openActionMenuId = itemId;
    }

    closeActionMenu() {
        this.openActionMenuId = null;
        this.actionMenuFlipUp = false;
    }

    get actionMenuClass() {
        return this.actionMenuFlipUp
            ? 'action-menu-portal flip-up'
            : 'action-menu-portal';
    }

    get actionMenuStyle() {
        return `top:${this.actionMenuPosition.top}px; left:${this.actionMenuPosition.left}px;`;
    }

    get openActionMenuItems() {
        const item = this.displayItems.find(row => row.id === this.openActionMenuId);
        if (!item) {
            return [];
        }

        return item.isDetailsExpanded
            ? [
                {
                    key: 'hide_details',
                    label: 'Hide details',
                    icon: 'utility:hide',
                    iconClass: 'action-icon-default'
                }
            ]
            : [
                {
                    key: 'view_details',
                    label: 'View details',
                    icon: 'utility:preview',
                    iconClass: 'action-icon-default'
                }
            ];
    }

    handleActionClick(event) {
        event.stopPropagation();

        const actionKey = event.currentTarget.dataset.action;
        const itemId = event.currentTarget.dataset.itemid;

        this.openActionMenuId = null;

        switch (actionKey) {
            case 'view_details':
            case 'hide_details':
                this.toggleDetailsExpansion(itemId);
                break;
            default:
                break;
        }
    }

    get totalImpressions() {
        return this.reconData
            ? Number(this.reconData.totalImpressions || 0).toLocaleString()
            : '0';
    }

    get totalRevenue() {
        return this.reconData
            ? this.formatCurrency(this.reconData.totalRevenue || 0)
            : '₹0.00';
    }

    get orderId() {
        return this.reconData ? this.reconData.orderId : 'N/A';
    }

    get invoiceId() {
        return this.reconData
            ? (this.reconData.invoiceName || this.reconData.invoiceId || 'N/A')
            : 'N/A';
    }

    get invoiceName() {
        return this.reconData
            ? (this.reconData.invoiceName || this.reconData.invoiceId || 'N/A')
            : 'N/A';
    }

    get advertiserName() {
        return this.reconData
            ? (this.reconData.advertiserName || 'Unknown')
            : 'Unknown';
    }

    get invoiceStartDate() {
        return this.reconData && this.reconData.invoiceStartDate
            ? this.formatDate(this.reconData.invoiceStartDate)
            : 'N/A';
    }

    get invoiceEndDate() {
        return this.reconData && this.reconData.invoiceEndDate
            ? this.formatDate(this.reconData.invoiceEndDate)
            : 'N/A';
    }

    get invoiceDate() {
        return this.reconData && this.reconData.invoiceDate
            ? this.formatDate(this.reconData.invoiceDate)
            : 'N/A';
    }

    get totalBilled() {
        return this.reconData
            ? this.formatCurrency(this.reconData.totalBilled || 0)
            : '₹0.00';
    }
}
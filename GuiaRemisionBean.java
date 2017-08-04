package sys.bean;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.servlet.ServletContext;
import org.primefaces.context.RequestContext;
import org.primefaces.event.SelectEvent;
import sys.clasesAuxiliares.reportesNotaDespacho;
import sys.dao.ConductorDAO;
import sys.dao.EmpresaTransporteDAO;
import sys.dao.EstadoTipoDocumentoDAO;
import sys.dao.FacturaDAO;
import sys.dao.FacturaDetalleDAO;
import sys.dao.GuiaRemisionDAO;
import sys.dao.GuiaRemisionDetalleDAO;
import sys.dao.MovimientoAlmacenDetalleDAO;
import sys.dao.MovimientoAlmacenDetalleTempDAO;
import sys.dao.NotaDespachoDAO;
import sys.dao.NotaDespachoDetalleDAO;
import sys.dao.ProductoStockMDAO;
import sys.dao.UnidadTransporteDAO;
import sys.imp.ConductorDAOImp;
import sys.imp.EmpresaTransporteDAOImp;
import sys.imp.EstadoTipoDocumentoDAOImp;
import sys.imp.FacturaDAOImp;
import sys.imp.FacturaDetalleDAOImp;
import sys.imp.GuiaRemisionDAOImp;
import sys.imp.GuiaRemisionDetalleDAOImp;
import sys.imp.MovimientoAlmacenDetalleDAOImp;
import sys.imp.MovimientoAlmacenDetalleTempDAOImp;
import sys.imp.NotaDespachoDAOImp;
import sys.imp.NotaDespachoDetalleDAOImp;
import sys.imp.ProductoStockMDAOImp;
import sys.imp.UnidadTransporteDAOImp;
import sys.model.AgmaeEstadoTipoDocumento;
import sys.model.AgmaePersona;
import sys.model.AimarConductores;
import sys.model.AimarEmpresaTransporte;
import sys.model.AimarMovAlmacenDet;
import sys.model.AimarStockm;
import sys.model.AimarUnidadTransporte;
import sys.model.AvmovFacturaNdCab;
import sys.model.AvmovFacturaNdDet;
import sys.model.AvmovGuiaRemisionCab;
import sys.model.AvmovGuiaRemisionDet;
import sys.model.AvmovMovNotaDespachoCab;
import sys.model.AvmovMovNotaDespachoDet;

@ManagedBean
@ViewScoped
public class GuiaRemisionBean implements Serializable {

    @ManagedProperty("#{loginBean}")
    private LoginBean lBean;
    private AvmovGuiaRemisionCab guiaRemision;
    private List<AvmovGuiaRemisionCab> listaGuiaRemision;

    private AvmovGuiaRemisionDet guiaRemisionDetalle;
    private List<AvmovGuiaRemisionDet> listaGuiaRemisionDetalle;

    private AvmovFacturaNdCab factura;
    private List<AvmovFacturaNdCab> listaFactura;
    private List<AvmovFacturaNdCab> seleccionFactura;
    private List<AvmovFacturaNdDet> listaFacturaDetalle;
    private List<AvmovFacturaNdDet> seleccionProductoFacturaDetalle;

    private AvmovMovNotaDespachoCab notaDespacho;
    private List<AvmovMovNotaDespachoCab> listaNotaDespacho;
    private List<AvmovMovNotaDespachoCab> seleccionNotaDespacho;
    private List<AvmovMovNotaDespachoDet> listaNotaDespachoDetalle;
    private List<AvmovMovNotaDespachoDet> seleccionProductoNotaDespachoDetalle;

    private List<AgmaeEstadoTipoDocumento> listaEstadoTipoDocumento;

    private List<AimarUnidadTransporte> listaUnidadTransporte;
    private AimarUnidadTransporte seleccionUnidadTransporte;

    private List<AimarConductores> listaConductor;
    private AimarConductores seleccionConductor;

    private List<AimarEmpresaTransporte> listaEmpresaTransporte;
    private AimarEmpresaTransporte seleccionEmpresaTransporte;

    private List<AimarStockm> seleccionStockProducto;

    private AvmovMovNotaDespachoDet notaDespachoDetalle;
    private List<AimarStockm> listaProductoStocks;

    private AgmaePersona seleccionCliente;

    //variables de filtro de listado
    private Date feDesde;
    private Date feHasta;
    private Date feMaxima;

    //variables de seleccion de producto
    private Date feDesdeAgregar;
    private Date feHastaAgregar;

    //variable de edicion
    private boolean nuevo;
    private List<AvmovGuiaRemisionDet> itemsEliminado;
    private String nameBtnSave;
    private String nameFocus;
    private boolean estadoNumeroSerie;

    public GuiaRemisionBean() {
        //Filtro fecha 
        Calendar calendar = Calendar.getInstance();
        feDesde = new Date();
        feHasta = new Date();
        feMaxima = new Date();
        calendar.add(Calendar.MONTH, -1);
        feDesde = calendar.getTime();

        //Fitro fecha pendientes 
        feDesdeAgregar = new Date();
        feHastaAgregar = new Date();
        feDesdeAgregar = calendar.getTime();

        //Listado
        listaGuiaRemision = new ArrayList<>();
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        listaGuiaRemision = gRDao.listarGuiasRemisionPorFecha(feDesde, feHasta);
    }

    public LoginBean getlBean() {
        return lBean;
    }

    public void setlBean(LoginBean lBean) {
        this.lBean = lBean;
    }

    public AvmovGuiaRemisionCab getGuiaRemision() {
        return guiaRemision;
    }

    public void setGuiaRemision(AvmovGuiaRemisionCab guiaRemision) {
        this.guiaRemision = guiaRemision;
    }

    public List<AvmovGuiaRemisionCab> getListaGuiaRemision() {
        return listaGuiaRemision;
    }

    public void setListaGuiaRemision(List<AvmovGuiaRemisionCab> listaGuiaRemision) {
        this.listaGuiaRemision = listaGuiaRemision;
    }

    public AvmovFacturaNdCab getFactura() {
        return factura;
    }

    public void setFactura(AvmovFacturaNdCab factura) {
        this.factura = factura;
    }

    public List<AvmovFacturaNdCab> getListaFactura() {
        return listaFactura;
    }

    public void setListaFactura(List<AvmovFacturaNdCab> listaFactura) {
        this.listaFactura = listaFactura;
    }

    public List<AvmovFacturaNdDet> getListaFacturaDetalle() {
        return listaFacturaDetalle;
    }

    public List<AvmovFacturaNdCab> getSeleccionFactura() {
        return seleccionFactura;
    }

    public void setSeleccionFactura(List<AvmovFacturaNdCab> seleccionFactura) {
        this.seleccionFactura = seleccionFactura;
    }

    public void setListaFacturaDetalle(List<AvmovFacturaNdDet> listaFacturaDetalle) {
        this.listaFacturaDetalle = listaFacturaDetalle;
    }

    public List<AvmovFacturaNdDet> getSeleccionProductoFacturaDetalle() {
        return seleccionProductoFacturaDetalle;
    }

    public void setSeleccionProductoFacturaDetalle(List<AvmovFacturaNdDet> seleccionProductoFacturaDetalle) {
        this.seleccionProductoFacturaDetalle = seleccionProductoFacturaDetalle;
    }

    public AvmovMovNotaDespachoCab getNotaDespacho() {
        return notaDespacho;
    }

    public void setNotaDespacho(AvmovMovNotaDespachoCab notaDespacho) {
        this.notaDespacho = notaDespacho;
    }

    public List<AvmovMovNotaDespachoCab> getListaNotaDespacho() {
        return listaNotaDespacho;
    }

    public void setListaNotaDespacho(List<AvmovMovNotaDespachoCab> listaNotaDespacho) {
        this.listaNotaDespacho = listaNotaDespacho;
    }

    public List<AvmovMovNotaDespachoCab> getSeleccionNotaDespacho() {
        return seleccionNotaDespacho;
    }

    public void setSeleccionNotaDespacho(List<AvmovMovNotaDespachoCab> seleccionNotaDespacho) {
        this.seleccionNotaDespacho = seleccionNotaDespacho;
    }

    public List<AvmovMovNotaDespachoDet> getListaNotaDespachoDetalle() {
        return listaNotaDespachoDetalle;
    }

    public void setListaNotaDespachoDetalle(List<AvmovMovNotaDespachoDet> listaNotaDespachoDetalle) {
        this.listaNotaDespachoDetalle = listaNotaDespachoDetalle;
    }

    public List<AvmovMovNotaDespachoDet> getSeleccionProductoNotaDespachoDetalle() {
        return seleccionProductoNotaDespachoDetalle;
    }

    public void setSeleccionProductoNotaDespachoDetalle(List<AvmovMovNotaDespachoDet> seleccionProductoNotaDespachoDetalle) {
        this.seleccionProductoNotaDespachoDetalle = seleccionProductoNotaDespachoDetalle;
    }

    public List<AimarStockm> getListaProductoStocks() {
        return listaProductoStocks;
    }

    public void setListaProductoStocks(List<AimarStockm> listaProductoStocks) {
        this.listaProductoStocks = listaProductoStocks;
    }

    
    
    public List<AgmaeEstadoTipoDocumento> getListaEstadoTipoDocumento() {
        EstadoTipoDocumentoDAO eTDDao = new EstadoTipoDocumentoDAOImp();
        listaEstadoTipoDocumento = eTDDao.listarEstadoTipoDocumento("09");
        return listaEstadoTipoDocumento;
    }

    public void setListaEstadoTipoDocumento(List<AgmaeEstadoTipoDocumento> listaEstadoTipoDocumento) {
        this.listaEstadoTipoDocumento = listaEstadoTipoDocumento;
    }

    public List<AimarUnidadTransporte> getListaUnidadTransporte() {
        UnidadTransporteDAO uTDao = new UnidadTransporteDAOImp();
        listaUnidadTransporte = uTDao.listarUnidadTransporte();
        return listaUnidadTransporte;
    }

    public void setListaUnidadTransporte(List<AimarUnidadTransporte> listaUnidadTransporte) {
        this.listaUnidadTransporte = listaUnidadTransporte;
    }

    public AimarUnidadTransporte getSeleccionUnidadTransporte() {
        return seleccionUnidadTransporte;
    }

    public void setSeleccionUnidadTransporte(AimarUnidadTransporte seleccionUnidadTransporte) {
        this.seleccionUnidadTransporte = seleccionUnidadTransporte;
    }

    public List<AimarConductores> getListaConductor() {
        ConductorDAO cDao = new ConductorDAOImp();
        listaConductor = cDao.listarConductores();
        return listaConductor;
    }

    public void setListaConductor(List<AimarConductores> listaConductor) {
        this.listaConductor = listaConductor;
    }

    public AimarConductores getSeleccionConductor() {
        return seleccionConductor;
    }

    public void setSeleccionConductor(AimarConductores seleccionConductor) {
        this.seleccionConductor = seleccionConductor;
    }

    public List<AimarEmpresaTransporte> getListaEmpresaTransporte() {
        EmpresaTransporteDAO eTDao = new EmpresaTransporteDAOImp();
        listaEmpresaTransporte = eTDao.listarEmpresaTransporte();
        return listaEmpresaTransporte;
    }

    public void setListaEmpresaTransporte(List<AimarEmpresaTransporte> listaEmpresaTransporte) {
        this.listaEmpresaTransporte = listaEmpresaTransporte;
    }

    public AimarEmpresaTransporte getSeleccionEmpresaTransporte() {
        return seleccionEmpresaTransporte;
    }

    public void setSeleccionEmpresaTransporte(AimarEmpresaTransporte seleccionEmpresaTransporte) {
        this.seleccionEmpresaTransporte = seleccionEmpresaTransporte;
    }

    public List<AimarStockm> getSeleccionStockProducto() {
        return seleccionStockProducto;
    }

    public void setSeleccionStockProducto(List<AimarStockm> seleccionStockProducto) {
        this.seleccionStockProducto = seleccionStockProducto;
    }

    public AvmovMovNotaDespachoDet getNotaDespachoDetalle() {
        return notaDespachoDetalle;
    }

    public void setNotaDespachoDetalle(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        this.notaDespachoDetalle = notaDespachoDetalle;
    }

    public AgmaePersona getSeleccionCliente() {
        return seleccionCliente;
    }

    public void setSeleccionCliente(AgmaePersona seleccionCliente) {
        this.seleccionCliente = seleccionCliente;
    }

    public void setGuiaRemisionDetalle(AvmovGuiaRemisionDet guiaRemisionDetalle) {
        this.guiaRemisionDetalle = guiaRemisionDetalle;
    }

    public List<AvmovGuiaRemisionDet> getListaGuiaRemisionDetalle() {
        return listaGuiaRemisionDetalle;
    }

    public void setListaGuiaRemisionDetalle(List<AvmovGuiaRemisionDet> listaGuiaRemisionDetalle) {
        this.listaGuiaRemisionDetalle = listaGuiaRemisionDetalle;
    }

    public Date getFeDesde() {
        return feDesde;
    }

    public void setFeDesde(Date feDesde) {
        this.feDesde = feDesde;
    }

    public Date getFeHasta() {
        return feHasta;
    }

    public void setFeHasta(Date feHasta) {
        this.feHasta = feHasta;
    }

    public Date getFeMaxima() {
        return feMaxima;
    }

    public void setFeMaxima(Date feMaxima) {
        this.feMaxima = feMaxima;
    }

    public Date getFeDesdeAgregar() {
        return feDesdeAgregar;
    }

    public void setFeDesdeAgregar(Date feDesdeAgregar) {
        this.feDesdeAgregar = feDesdeAgregar;
    }

    public Date getFeHastaAgregar() {
        return feHastaAgregar;
    }

    public void setFeHastaAgregar(Date feHastaAgregar) {
        this.feHastaAgregar = feHastaAgregar;
    }

    public boolean isNuevo() {
        return nuevo;
    }

    public void setNuevo(boolean nuevo) {
        this.nuevo = nuevo;
    }

    public List<AvmovGuiaRemisionDet> getItemsEliminado() {
        return itemsEliminado;
    }

    public void setItemsEliminado(List<AvmovGuiaRemisionDet> itemsEliminado) {
        this.itemsEliminado = itemsEliminado;
    }

    public String getNameBtnSave() {
        return nameBtnSave;
    }

    public void setNameBtnSave(String nameBtnSave) {
        this.nameBtnSave = nameBtnSave;
    }

    public String getNameFocus() {
        return nameFocus;
    }

    public void setNameFocus(String nameFocus) {
        this.nameFocus = nameFocus;
    }

    public boolean isEstadoNumeroSerie() {
        return estadoNumeroSerie;
    }

    public void setEstadoNumeroSerie(boolean estadoNumeroSerie) {
        this.estadoNumeroSerie = estadoNumeroSerie;
    }

    public void agregarProductoGuiaRemisionDetalle() {
        MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();

        AvmovMovNotaDespachoCab nd=new AvmovMovNotaDespachoCab();
       nd.setIdMovValeCab(1);
       nd.setNumVale("A00000");
       nd.setFecVale(guiaRemision.getFecEmision());
        for (AimarStockm prod : seleccionStockProducto) {
            notaDespachoDetalle = new AvmovMovNotaDespachoDet();
            notaDespachoDetalle.setIdMovValeCab(notaDespacho.getIdMovValeCab());
            notaDespachoDetalle.setNumVale(notaDespacho.getNumVale());

            notaDespachoDetalle.setRucCompanyia(prod.getRuc());
            notaDespachoDetalle.setCodEstablecimiento(prod.getCodEstablecimiento());
            notaDespachoDetalle.setCodCentroc(prod.getCodCentro());
            notaDespachoDetalle.setCodArea(prod.getCodArea());
            notaDespachoDetalle.setCodProducto(prod.getCodProducto());
            notaDespachoDetalle.setCodAlmacen(prod.getCodAlmacen());
            notaDespachoDetalle.setCodPresentacion(prod.getCodPresentacion());
            notaDespachoDetalle.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());

            mADTDao.newMovimientoAlmacenDetalleTemp(notaDespachoDetalle);
        }
        
        listaNotaDespachoDetalle = mADTDao.listarNotaDespachoDetalles(notaDespacho);
        for (AvmovMovNotaDespachoDet item : listaNotaDespachoDetalle) {
            guiaRemisionDetalle = new AvmovGuiaRemisionDet();
            guiaRemisionDetalle.setNumCantidadProducto(item.getCtdMovimiento());
            guiaRemisionDetalle.setCodProducto(item.getCodProducto());
            guiaRemisionDetalle.setCodMedida(item.getCodMedida());
            guiaRemisionDetalle.setCodTipoDocumentoOrigen("99");
            guiaRemisionDetalle.setZnomTipoDocumentoOrigen("DIRECTO");
            guiaRemisionDetalle.setIdOrigen(item.getIdMovValeProducto());
            guiaRemisionDetalle.setZnomProducto(item.getNomProducto());
            guiaRemisionDetalle.setNuevo(true);
            guiaRemisionDetalle.setEditado(false);

            listaGuiaRemisionDetalle.add(guiaRemisionDetalle);
            
        }
    }

    public void agregarFacturaDetalleAGuiaRemisionDetalle() {
        for (AvmovFacturaNdDet item : seleccionProductoFacturaDetalle) {
            guiaRemisionDetalle = new AvmovGuiaRemisionDet();
            guiaRemisionDetalle.setNumCantidadProducto(item.getNumCntdProducto());
            guiaRemisionDetalle.setCodProducto(item.getCodProducto());
            guiaRemisionDetalle.setCodMedida(item.getCodMedida());
            guiaRemisionDetalle.setCodTipoDocumentoOrigen("01");
            guiaRemisionDetalle.setZnomTipoDocumentoOrigen("FACTURA");
            guiaRemisionDetalle.setIdOrigen(item.getIdFacturaDet());
            guiaRemisionDetalle.setZnomProducto(item.getZnomProducto());
            guiaRemisionDetalle.setNuevo(true);
            guiaRemisionDetalle.setEditado(false);

            listaGuiaRemisionDetalle.add(guiaRemisionDetalle);
        }
    }

    public void agregarNotaDespachoDetalleAGuiaRemisionDetalle() {
        for (AvmovMovNotaDespachoDet item : seleccionProductoNotaDespachoDetalle) {
            guiaRemisionDetalle = new AvmovGuiaRemisionDet();
            guiaRemisionDetalle.setNumCantidadProducto(item.getCtdMovimiento());
            guiaRemisionDetalle.setCodProducto(item.getCodProducto());
            guiaRemisionDetalle.setCodMedida(item.getCodMedida());
            guiaRemisionDetalle.setCodTipoDocumentoOrigen("09");
            guiaRemisionDetalle.setZnomTipoDocumentoOrigen("NOTA DE DESPACHO");
            guiaRemisionDetalle.setIdOrigen(item.getIdMovValeProducto());
            guiaRemisionDetalle.setZnomProducto(item.getNomProducto());
            guiaRemisionDetalle.setNuevo(true);
            guiaRemisionDetalle.setEditado(false);

            listaGuiaRemisionDetalle.add(guiaRemisionDetalle);
        }
    }

    public void anularGuiaRemision(AvmovGuiaRemisionCab gr) {
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        gr = gRDao.obtenerGuiaRemisionPorIdGuiaRemision(gr);
        gr.setFlgEstado("A");
        gr.setCodUsuarioActualizacion(lBean.getUsuario().getCodUsuario());
        gRDao.updateGuiaRemision(gr);
        listaGuiaRemision = gRDao.listarGuiasRemisionPorFecha(feDesde, feHasta);
        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Se anulo la Guia de Remision #" + gr.getNumSerie() + "-" + gr.getNumGuia()));
    }

    public void consultarFacturasPorFechas() {
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        listaFactura = gRDao.listaFacturas(feDesdeAgregar, feHastaAgregar, listaGuiaRemisionDetalle);
    }

    public void consultarNotaDespachosPorFechas() {
        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        listaNotaDespacho = nDDao.listaNotaDespachos(feDesdeAgregar, feHastaAgregar, listaGuiaRemisionDetalle);
    }

    public void consultarPorFechas() {
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        listaGuiaRemision = gRDao.listarGuiasRemisionPorFecha(feDesde, feHasta);
    }

    public void guardarGuiaRemision() {
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        GuiaRemisionDetalleDAO gRDDao = new GuiaRemisionDetalleDAOImp();

//        MovimientoAlmacenDetalleDAO mADDao = new MovimientoAlmacenDetalleDAOImp();
//        AimarMovAlmacenDet mAD = new AimarMovAlmacenDet();
//        NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();
        if (isNuevo()) {
            if (guiaRemision.getNumSerie().length() > 0) {

                guiaRemision.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                gRDao.newGuiaRemision(guiaRemision);

                guiaRemision.setIdGuiaRemisionCab(gRDao.obtenerIdGuiaRemisionPorNumGuiaRemision(guiaRemision));
                for (AvmovGuiaRemisionDet item : listaGuiaRemisionDetalle) {
                    item.setIdGuiaRemisionCab(guiaRemision.getIdGuiaRemisionCab());
                    item.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                    gRDDao.newGuiaRemisionDetalle(item);

                }

                listaGuiaRemision = gRDao.listarGuiasRemisionPorFecha(feDesde, feHasta);

                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(
                        FacesMessage.SEVERITY_INFO,
                        "Informacion",
                        "Se guardo con exito la guia de remision #" + guiaRemision.getNumSerie() + "-" + guiaRemision.getNumGuia() + "! "));
                RequestContext.getCurrentInstance().execute("PF('dialogNuevaGuiaRemision').hide();");

            } else {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Falta ingresar el numero de serie y asi poder generar el numero de guia de remision"));
            }
        } else {
            if (guiaRemision.getNumSerie().length() > 0) {
                guiaRemision.setCodUsuarioActualizacion(lBean.getUsuario().getCodUsuario());
                gRDao.updateGuiaRemision(guiaRemision);
                for (AvmovGuiaRemisionDet item : listaGuiaRemisionDetalle) {
                    if (item.isNuevo()) {
                        item.setIdGuiaRemisionCab(guiaRemision.getIdGuiaRemisionCab());
                        item.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                        gRDDao.newGuiaRemisionDetalle(item);

                    } else {
                        if (item.isEditado()) {
                            gRDDao.updateGuiaRemisionDetalle(item);
                        }
                    }
                }
                for (AvmovGuiaRemisionDet item : itemsEliminado) {
                    gRDDao.deleteGuiaRemisionDetalle(item);
                }
                listaGuiaRemision = gRDao.listarGuiasRemisionPorFecha(feDesde, feHasta);

                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO,
                        "Informacion",
                        "Se guardo con exito la guia de remision #" + guiaRemision.getNumSerie() + "-" + guiaRemision.getNumGuia() + "!"));
                RequestContext.getCurrentInstance().execute("PF('dialogNuevaGuiaRemision').hide();");
            } else {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Falta ingresar el numero de la Guia de Remision"));
            }
        }
    }

    public void generarNroGuia() {
        estadoNumeroSerie = true;
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        guiaRemision.setNumGuia(gRDao.generarNroGuiaRemision(guiaRemision));

    }

    public void listarProductosFacturaDetalle(AvmovFacturaNdCab factura) {
        listaFacturaDetalle = new ArrayList<>();
        FacturaDetalleDAO fDDao = new FacturaDetalleDAOImp();
        listaFacturaDetalle = fDDao.listarFacturaDetalleSinGR(factura, listaGuiaRemisionDetalle);
    }

    public void listarProductosNotaDespachoDetalle(AvmovMovNotaDespachoCab notaDespacho) {
        listaNotaDespachoDetalle = new ArrayList<>();
        NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();
        listaNotaDespachoDetalle = nDDDao.listarNotaDespachoDetalleSinGR(notaDespacho, listaGuiaRemisionDetalle);
    }

    public void listoCliente() {
        guiaRemision.setCodPersona(seleccionCliente.getCodPersona());
        guiaRemision.setZrucPersona(seleccionCliente.getNumIdentificacion());
        guiaRemision.setZnomPersona(seleccionCliente.getNomRazonSocial());
        

    }

    public void listoUnidadTransporte() {
        guiaRemision.setNumPlaca(seleccionUnidadTransporte.getPlaca());
        guiaRemision.setZnomMarca(seleccionUnidadTransporte.getMarca());
        guiaRemision.setZnomConstancia(seleccionUnidadTransporte.getConstancia());

    }

    public void listoConductor() {
        guiaRemision.setDniConductor(seleccionConductor.getDniCon());
        guiaRemision.setZnomConductor(seleccionConductor.getDesCon());
        guiaRemision.setZlicConductor(seleccionConductor.getBreCon());

    }

    public void listoEmpresaTransporte() {
        guiaRemision.setCodEmptransporte(seleccionEmpresaTransporte.getCodEmptransporte());
        guiaRemision.setZrucEmpresa(seleccionEmpresaTransporte.getRucEmptransporte());
        guiaRemision.setZnomEmpresa(seleccionEmpresaTransporte.getNomEmptransporte());
        guiaRemision.setZnumTelefono(seleccionEmpresaTransporte.getNumTelefono());
    }

    public void prepararNuevaGuiaRemision() {
        //Tipo de registro
        setEstadoNumeroSerie(false);
        setNuevo(true);
        nameBtnSave = "Guardar";
        nameFocus = "numSerie";

        guiaRemision = new AvmovGuiaRemisionCab();
        listaGuiaRemisionDetalle = new ArrayList<>();

        guiaRemision.setFecEmision(new Date());
        guiaRemision.setFecInicioTraslado(new Date());
        guiaRemision.setCodTipoDocumento("09");
        guiaRemision.setRucCompanyia(lBean.getCompanya().getRucCompanya());
        guiaRemision.setZnomCompanyia(lBean.getCompanya().getDesCompanyia());

        guiaRemisionDetalle = new AvmovGuiaRemisionDet();
        itemsEliminado = new ArrayList<>();
    }

    public void prepararEditarGuiaRemision(AvmovGuiaRemisionCab gr) {
        setEstadoNumeroSerie(true);
        setNuevo(false);
        nameBtnSave = "Actualizar";
        nameFocus = "tablaGuiaRemisionDetalle";

        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        GuiaRemisionDetalleDAO gRDDao = new GuiaRemisionDetalleDAOImp();

        guiaRemision = new AvmovGuiaRemisionCab();
        guiaRemision = gRDao.obtenerGuiaRemisionPorIdGuiaRemision(gr);

        listaGuiaRemisionDetalle = new ArrayList<>();
        listaGuiaRemisionDetalle = gRDDao.listarGuiasRemisionDetalle(gr);

        itemsEliminado = new ArrayList();
    }

    public void prepararAgregarProducto() {
                    ProductoStockMDAO pSMDao = new ProductoStockMDAOImp();
                    listaProductoStocks = pSMDao.listarProductoStocks(guiaRemision.getFecEmision());

    }

    public void prepararAgregarProductosFactura() {
        factura = new AvmovFacturaNdCab();
        listaFactura = new ArrayList<>();
        listaFacturaDetalle = new ArrayList<>();
        GuiaRemisionDAO gRDao = new GuiaRemisionDAOImp();
        listaFactura = gRDao.listaFacturas(feDesdeAgregar, feHastaAgregar, listaGuiaRemisionDetalle);

    }

    public void prepararAgregarProductosNotaDespacho() {
        notaDespacho = new AvmovMovNotaDespachoCab();
        listaNotaDespacho = new ArrayList<>();
        listaNotaDespachoDetalle = new ArrayList<>();
        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        listaNotaDespacho = nDDao.listaNotaDespachos(feDesdeAgregar, feHastaAgregar, listaGuiaRemisionDetalle);

    }

    public void onRowSelect(SelectEvent event) {
        listarProductosFacturaDetalle((AvmovFacturaNdCab) event.getObject());
    }

    public void onRowSelectND(SelectEvent event) {
        listarProductosNotaDespachoDetalle((AvmovMovNotaDespachoCab) event.getObject());
    }

    public void quitarGuiaRemisionDetalle(AvmovGuiaRemisionDet grd) {
        if (grd.isNuevo() == false) {
            itemsEliminado.add(grd);
        }
        listaGuiaRemisionDetalle.remove(grd);
    }

    public void verReporteGuiaRemision(AvmovGuiaRemisionCab gr) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        reportesNotaDespacho rGuiaRemision = new reportesNotaDespacho();

        FacesContext facesContext = FacesContext.getCurrentInstance();
        ServletContext servletContext = (ServletContext) facesContext.getExternalContext().getContext();
        String ruta = servletContext.getRealPath("/reports/rpt_GuiaRemision.jasper");

        rGuiaRemision.getReporteGuiaRemision(ruta, gr.getIdGuiaRemisionCab());
        FacesContext.getCurrentInstance().responseComplete();

    }

}

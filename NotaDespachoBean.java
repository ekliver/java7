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
import sys.clasesAuxiliares.reportesNotaDespacho;
import sys.dao.EstadoTipoDocumentoDAO;
import sys.dao.MovimientoAlmacenDAO;
import sys.dao.MovimientoAlmacenDetalleDAO;
import sys.dao.MovimientoAlmacenDetalleTempDAO;
import sys.dao.MovimientoAlmacenTempDAO;
import sys.dao.NotaDespachoDAO;
import sys.dao.NotaDespachoDetalleDAO;
import sys.dao.ProductoStockMDAO;
import sys.dao.VendedorDAO;
import sys.imp.EstadoTipoDocumentoDAOImp;
import sys.imp.MovimientoAlmacenDAOImp;
import sys.imp.MovimientoAlmacenDetalleDAOImp;
import sys.imp.MovimientoAlmacenDetalleTempDAOImp;
import sys.imp.MovimientoAlmacenTempDAOImp;
import sys.imp.NotaDespachoDAOImp;
import sys.imp.NotaDespachoDetalleDAOImp;
import sys.imp.ProductoStockMDAOImp;
import sys.imp.VendedorDAOImp;
import sys.model.AgmaeEstadoTipoDocumento;
import sys.model.AgmaePersona;
import sys.model.AgmaeVendedor;
import sys.model.AimarMovAlmacenCab;
import sys.model.AimarMovAlmacenDet;
import sys.model.AimarStockm;
import sys.model.AvmovMovNotaDespachoCab;
import sys.model.AvmovMovNotaDespachoDet;

@ManagedBean
@ViewScoped
public class NotaDespachoBean implements Serializable {

    @ManagedProperty("#{loginBean}")
    private LoginBean lBean;

    private AvmovMovNotaDespachoCab notaDespacho;
    private List<AvmovMovNotaDespachoCab> listaNotaDespacho;

    private AvmovMovNotaDespachoDet notaDespachoDetalle;
    private List<AvmovMovNotaDespachoDet> listaNotaDespachoDetalle;

    private AimarStockm stockProducto;
    private List<AimarStockm> seleccionStockProducto;

    private List<AimarStockm> listaProductoStocks;

    private List<AgmaeVendedor> listaVendedores;
    private AgmaeVendedor seleccionVendedor;

    private List<AgmaeEstadoTipoDocumento> listaEstadoTipoDocumento;

    private AgmaePersona seleccionCliente;

    //variables de filtro de listado
    private Date feDesde;
    private Date feHasta;
    private Date feMaxima;

    //variables de edicion
    private boolean enabledND;
//    private Integer estadoEdicion;
    private boolean nuevo;
    private String nameBtnSave;
    private String nameFocus;

    public NotaDespachoBean() {
        //Filtro de fecha
        Calendar calendar = Calendar.getInstance();
        feDesde = new Date();
        feHasta = new Date();
        feMaxima = new Date();
        calendar.add(Calendar.MONTH, -1);
        feDesde = calendar.getTime();

        //Listado
        listaNotaDespacho = new ArrayList<>();
        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        listaNotaDespacho = nDDao.listarNotaDespachosPorFecha(feDesde, feHasta);

        notaDespacho = new AvmovMovNotaDespachoCab();
    }

    public LoginBean getlBean() {
        return lBean;
    }

    public void setlBean(LoginBean lBean) {
        this.lBean = lBean;
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

    public AvmovMovNotaDespachoDet getNotaDespachoDetalle() {
        return notaDespachoDetalle;
    }

    public void setNotaDespachoDetalle(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        this.notaDespachoDetalle = notaDespachoDetalle;
    }

    public List<AvmovMovNotaDespachoDet> getListaNotaDespachoDetalle() {

        return listaNotaDespachoDetalle;
    }

    public void setListaNotaDespachoDetalle(List<AvmovMovNotaDespachoDet> listaNotaDespachoDetalle) {
        this.listaNotaDespachoDetalle = listaNotaDespachoDetalle;
    }

    public AimarStockm getStockProducto() {
        return stockProducto;
    }

    public void setStockProducto(AimarStockm stockProducto) {
        this.stockProducto = stockProducto;
    }

    public List<AimarStockm> getSeleccionStockProducto() {
        return seleccionStockProducto;
    }

    public void setSeleccionStockProducto(List<AimarStockm> seleccionStockProducto) {
        this.seleccionStockProducto = seleccionStockProducto;
    }

    public List<AimarStockm> getListaProductoStocks() {
        return listaProductoStocks;
    }

    public void setListaProductoStocks(List<AimarStockm> listaProductoStocks) {
        this.listaProductoStocks = listaProductoStocks;
    }

    public List<AgmaeVendedor> getListaVendedores() {
        VendedorDAO vDao = new VendedorDAOImp();
        listaVendedores = vDao.listarVendedores();
        return listaVendedores;
    }

    public void setListaVendedores(List<AgmaeVendedor> listaVendedores) {
        this.listaVendedores = listaVendedores;
    }

    public AgmaeVendedor getSeleccionVendedor() {
        return seleccionVendedor;
    }

    public void setSeleccionVendedor(AgmaeVendedor seleccionVendedor) {
        this.seleccionVendedor = seleccionVendedor;
    }

    public AgmaePersona getSeleccionCliente() {
        return seleccionCliente;
    }

    public void setSeleccionCliente(AgmaePersona seleccionCliente) {
        this.seleccionCliente = seleccionCliente;
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

    public boolean isEnabledND() {
        return enabledND;
    }

    public void setEnabledND(boolean enabledND) {
        this.enabledND = enabledND;
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

    public boolean isNuevo() {
        return nuevo;
    }

    public void setNuevo(boolean nuevo) {
        this.nuevo = nuevo;
    }

    public List<AgmaeEstadoTipoDocumento> getListaEstadoTipoDocumento() {
        EstadoTipoDocumentoDAO eTDDao = new EstadoTipoDocumentoDAOImp();
        listaEstadoTipoDocumento = eTDDao.listarEstadoTipoDocumento("88");
        return listaEstadoTipoDocumento;
    }

    public void setListaEstadoTipoDocumento(List<AgmaeEstadoTipoDocumento> listaEstadoTipoDocumento) {
        this.listaEstadoTipoDocumento = listaEstadoTipoDocumento;
    }

    public void agregarNotasDespachoDetalle() {
        if (isNuevo()) {
            MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();

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

            }//Actualizacion por consulta
            listaNotaDespachoDetalle = mADTDao.listarNotaDespachoDetalles(notaDespacho);
        } else {
            NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();
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
                notaDespachoDetalle.setNomPresentacion(prod.getNomPresentacion());
                //notaDespachoDetalle.setCtdMovimiento(1);
                notaDespachoDetalle.setNuevo(true);
                notaDespachoDetalle.setEditado(false);
                
                nDDDao.newNotaDespachoDetalle(notaDespachoDetalle);
                //Actualizacion por consulta
                listaNotaDespachoDetalle = nDDDao.listarNotaDespachoDetalles(notaDespacho);
            }

        }
    }

    public void anularNotaDespacho(AvmovMovNotaDespachoCab nd) {

        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        nd.setFlgEstado("A");
        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Se ha anulado la nota de despacho #" + nd.getNumVale()));
        nDDao.updateNotaDespacho(nd);

        listaNotaDespacho = nDDao.listarNotaDespachosPorFecha(feDesde, feHasta);
    }

    public void consultarPorFechas() {
        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        listaNotaDespacho = nDDao.listarNotaDespachosPorFecha(feDesde, feHasta);
    }

    public void guardarNotaDespacho() {
        if (isNuevo()) {
            try {

                NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
                int validarCantidad = 0;
                String itemsSinCantidad = "";

                if (nDDao.obtenerNotaDespacho(notaDespacho).getIdMovValeCab() == 0) {
                    if (notaDespacho.getCodPersona() > 0) {
                        int i = 0;
                        for (AvmovMovNotaDespachoDet it : listaNotaDespachoDetalle) {
                            i++;
                            if (it.getCtdMovimiento() <= 0) {

                                validarCantidad += 1;
                                if (validarCantidad == 1) {
                                    itemsSinCantidad = String.valueOf(i);
                                } else {
                                    itemsSinCantidad = itemsSinCantidad + ", " + i;
                                }

                            }
                        }
                        if (validarCantidad == 0) {

                            notaDespacho.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                            notaDespacho.setFlgEstado("P");
                            nDDao.newNotaDespacho(notaDespacho);
                            notaDespacho.setIdMovValeCab(nDDao.obtenerNotaDespacho(notaDespacho).getIdMovValeCab());

                            /* Guardando la cabecera de movimimiento de almacen */
                            AimarMovAlmacenCab movimientoAlmacen = new AimarMovAlmacenCab();
                            movimientoAlmacen.setRucCompanyia(notaDespacho.getRucCompanyia());
                            movimientoAlmacen.setAnyo(lBean.getAnyo());
                            movimientoAlmacen.setCodEstablecimiento(notaDespacho.getCodEstablecimiento());
                            movimientoAlmacen.setCodCentroc(notaDespacho.getCodCentroc());
                            movimientoAlmacen.setCodArea(notaDespacho.getCodArea());
                            movimientoAlmacen.setCodAlmacen(notaDespacho.getCodAlmacen());
                            movimientoAlmacen.setCodTipoProducto("MT");
                            movimientoAlmacen.setCodTipoConcepto("S");
                            movimientoAlmacen.setNumDocumento(notaDespacho.getNumVale());
                            movimientoAlmacen.setCodPersona(notaDespacho.getCodPersona());
                            movimientoAlmacen.setFecMovimiento(notaDespacho.getFecVale());
                            movimientoAlmacen.setCodConcepto("24");
                            movimientoAlmacen.setCodDocumento("88");
                            movimientoAlmacen.setCodUsuarioCreacion(notaDespacho.getCodUsuarioCreacion());

                            MovimientoAlmacenDAO mADao = new MovimientoAlmacenDAOImp();
                            AimarMovAlmacenCab mv;
                            NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();
                            MovimientoAlmacenDetalleDAO mADDao = new MovimientoAlmacenDetalleDAOImp();
                            AimarMovAlmacenDet movimientoAlmacenDetalle;

                            for (AvmovMovNotaDespachoDet item : listaNotaDespachoDetalle) {
                                String numMovimieto = mADao.obtenerNroMovimientoRelNdMovInv(item);

                                if (numMovimieto.equals("")) {//validar si compañia no tiene movimiento
                                    mADao.newMovimientoAlmacen(movimientoAlmacen, notaDespacho);
                                }
                                mv = mADao.obtenerMovimientoAlmacen(movimientoAlmacen);
                                movimientoAlmacen.setIdMovAlmCab(mv.getIdMovAlmCab());
                                movimientoAlmacen.setNumMovimiento(mv.getNumMovimiento());

                                item.setIdMovValeCab(notaDespacho.getIdMovValeCab());

                                nDDDao.newNotaDespachoDetalle(item);
                                movimientoAlmacenDetalle = new AimarMovAlmacenDet();
                                movimientoAlmacenDetalle.setIdMovAlmCab(movimientoAlmacen.getIdMovAlmCab());
                                movimientoAlmacenDetalle.setIdNotaDespachoDet(item.getIdMovValeProducto());
                                movimientoAlmacenDetalle.setRucCompanyia(movimientoAlmacen.getRucCompanyia());
                                movimientoAlmacenDetalle.setAnyo(movimientoAlmacen.getAnyo());
                                movimientoAlmacenDetalle.setCodEstablecimiento(item.getCodEstablecimiento());
                                movimientoAlmacenDetalle.setCodCentroc(item.getCodCentroc());
                                movimientoAlmacenDetalle.setCodArea(item.getCodArea());
                                movimientoAlmacenDetalle.setCodAlmacen(item.getCodAlmacen());
                                movimientoAlmacenDetalle.setNumMovimiento(movimientoAlmacen.getNumMovimiento());
                                movimientoAlmacenDetalle.setCodProducto(item.getCodProducto());
                                movimientoAlmacenDetalle.setCodTipoProducto(movimientoAlmacen.getCodTipoProducto());
                                movimientoAlmacenDetalle.setNumCantidad(item.getCtdMovimiento());
                                movimientoAlmacenDetalle.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                                movimientoAlmacenDetalle.setCodPresentacion(item.getCodPresentacion());
                                movimientoAlmacenDetalle.setNumCantidadPresentacion(item.getNumCantidadPresentacion());
                                movimientoAlmacenDetalle.setCodUm(item.getCodMedida());
                                movimientoAlmacenDetalle.setValEquivalencia(item.getNumPresentacionEquivalencia());
                                movimientoAlmacenDetalle.setNumCantidadPresentacion(item.getNumCantidadPresentacion());

                                mADDao.newMovimientoAlmacenDetalle(movimientoAlmacenDetalle);

                            }

                            listaNotaDespacho = nDDao.listarNotaDespachosPorFecha(feDesde, feHasta);
                            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO,
                                    "Informacion", "Se guardo con exito!"));
                            RequestContext.getCurrentInstance().execute("PF('dialogNuevaNotaDespacho').hide();");
                            MovimientoAlmacenTempDAO mATDao = new MovimientoAlmacenTempDAOImp();
                            mATDao.deleteAllMovimientoAlmacen();

                            MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();
                            mADTDao.deleteAllMovimientoAlmacenDetalleTemp(lBean.getUsuario());

                        } else {
                            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN,
                                    "Informacion", "Debe indicar cantidad para los siguientes items:\n" + itemsSinCantidad));
                        }

                    } else {
                        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN,
                                "Informacion", "Debe seleccionar un cliente"));

                    }
                } else {
                    FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN,
                            "Informacion", "La nota de despacho #" + notaDespacho.getNumVale() + " ya existe"));
                }

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        } else {
            try {

                NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
                notaDespacho.setCodUsuarioActualizacion(lBean.getUsuario().getCodUsuario());
                nDDao.updateNotaDespacho(notaDespacho);
                listaNotaDespacho = nDDao.listarNotaDespachosPorFecha(feDesde, feHasta);
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO,
                        "Informacion", "Se guardo con exito!"));
                RequestContext.getCurrentInstance().execute("PF('dialogNuevaNotaDespacho').hide();");
                MovimientoAlmacenTempDAO mATDao = new MovimientoAlmacenTempDAOImp();
                mATDao.deleteAllMovimientoAlmacen();

                MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();
                mADTDao.deleteAllMovimientoAlmacenDetalleTemp(lBean.getUsuario());
            } catch (Exception e) {

                System.out.println(e.getMessage());

            }
        }

    }

    public void listoVendedor() {
        notaDespacho.setCodVendedor(seleccionVendedor.getCodVendedor());
        notaDespacho.setAgmaeVendedor(seleccionVendedor);

    }

    public void listoCliente() {
        notaDespacho.setCodPersona(seleccionCliente.getCodPersona());
        notaDespacho.setAgmaePersona(seleccionCliente);

    }

    public void prepararNuevaNotaDespacho() {
        //Tipo de registro
        setNuevo(true);
        setEnabledND(false);
        nameBtnSave = "Guardar";
        nameFocus = "numND";

        //Limpieza de tablas temporrales
        MovimientoAlmacenTempDAO mATDao = new MovimientoAlmacenTempDAOImp();
        mATDao.deleteAllMovimientoAlmacen();
        MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();
        mADTDao.deleteAllMovimientoAlmacenDetalleTemp(lBean.getUsuario());

        notaDespacho = new AvmovMovNotaDespachoCab();
        listaNotaDespachoDetalle = new ArrayList<>();

        notaDespacho.setRucCompanyia(lBean.getUsuario().getRucCompanyia());
        notaDespacho.setCodEstablecimiento(lBean.getUsuario().getCodEstablecimiento());
        notaDespacho.setCodArea(lBean.getUsuario().getCodArea());
        notaDespacho.setCodCentroc(lBean.getUsuario().getCodCentroc());
        notaDespacho.setCodAlmacen(lBean.getUsuario().getCodAlmacen());
        notaDespacho.setFecVale(new Date());
        notaDespacho.setCodPrioridad(1);
        notaDespacho.setCodTipoDocumento("88");

        VendedorDAO vDao = new VendedorDAOImp();
        notaDespacho.setAgmaeVendedor(vDao.consultarObjVendedor());
        notaDespacho.setCodVendedor(notaDespacho.getAgmaeVendedor().getCodVendedor());

        seleccionCliente = new AgmaePersona();
    }

    public void prepararEditarNotaDespacho(AvmovMovNotaDespachoCab nd) {
        setNuevo(false);
        setEnabledND(true);
        nameBtnSave = "Actualizar";
        nameFocus = "tablaNotaDespachoDetalle";

        NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
        NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();

        notaDespacho = new AvmovMovNotaDespachoCab();
        notaDespacho = nDDao.obtenerNotaDespacho(nd);

        listaNotaDespachoDetalle = new ArrayList<>();
        listaNotaDespachoDetalle = nDDDao.listarNotaDespachoDetalles(nd);

        

        seleccionCliente = new AgmaePersona();
    }

    public void prepararAgregarProducto() {
        if (isNuevo()) {
            if (notaDespacho.getNumVale() != null && notaDespacho.getNumVale() != "") {

                NotaDespachoDAO nDDao = new NotaDespachoDAOImp();
                if (nDDao.obtenerNotaDespacho(notaDespacho).getIdMovValeCab() == 0) {
                    setEnabledND(true);
                    stockProducto = new AimarStockm();
                    seleccionStockProducto = new ArrayList<>();
                    if (notaDespacho.getIdMovValeCab() == 0) {
                        notaDespacho.setCodUsuarioCreacion(lBean.getUsuario().getCodUsuario());
                        notaDespacho.setFlgEstado("P");
                        MovimientoAlmacenTempDAO mADTDao = new MovimientoAlmacenTempDAOImp();

                        mADTDao.newMovimientoAlmacen(notaDespacho);
                        notaDespacho.setIdMovValeCab(mADTDao.obtenerMovimientoAlmacen(notaDespacho).getIdMovValeCab());
                    }

                    ProductoStockMDAO pSMDao = new ProductoStockMDAOImp();
                    listaProductoStocks = pSMDao.listarProductoStocks(notaDespacho.getFecVale());
                    

                    RequestContext.getCurrentInstance().execute("PF('dialogAgregarProducto').show();");
                } else {
                    FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_FATAL,
                            "Informacion", "La nota de despacho #" + notaDespacho.getNumVale() + " ya existe"));
                }

            } else {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_ERROR, "Informacion", "falta agregar el numero de despacho"));

            }
        } else {

            stockProducto = new AimarStockm();
            seleccionStockProducto = new ArrayList<>();
            ProductoStockMDAO pSMDao = new ProductoStockMDAOImp();
            listaProductoStocks = pSMDao.listarProductoStocks(notaDespacho.getFecVale());
            RequestContext.getCurrentInstance().execute("PF('dialogAgregarProducto').show();");
        }
    }

    public void onCellEdit(Integer capturaIdx, AvmovMovNotaDespachoDet ndd) {

        MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();
        MovimientoAlmacenDAO mADao = new MovimientoAlmacenDAOImp();
        NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();

        int i = 0;
        for (AvmovMovNotaDespachoDet item : listaNotaDespachoDetalle) {
            if (i == capturaIdx) {

                if (ndd.getRucCompanyia().equals(item.getRucCompanyia())
                        && ndd.getCodEstablecimiento().equals(item.getCodEstablecimiento())
                        && ndd.getCodArea() == item.getCodArea() && ndd.getCodCentroc() == item.getCodCentroc()
                        && ndd.getCodProducto().equals(item.getCodProducto()) && ndd.getCodAlmacen() == item.getCodAlmacen()) {
                    if (ndd.getNumCantidadPresentacion() > 0) {
                        item.setCtdMovimiento(item.getNumCantidadPresentacion() * ndd.getNumPresentacionEquivalencia());
                        // FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Se ha eliminado la nota de despacho #"));
                        //item.setNumCantidadPresentacion(0);

                    }

                    if (isNuevo()) {
                        mADTDao.updateMovimientoAlmacenDetalleTemp(item);
                    } else {
                        nDDDao.updateNotaDespachoDetalle(item);

                        // mADao.updateMovimientoAlmacen(item);
                    }
                }
            }
            i++;
        }
        if (isNuevo()) {
            listaNotaDespachoDetalle = mADTDao.listarNotaDespachoDetalles(notaDespacho);
        } else {
            listaNotaDespachoDetalle = nDDDao.listarNotaDespachoDetalles(notaDespacho);

        }

    }

    public void quitarNotaDespachodetalle(AvmovMovNotaDespachoDet ndd, Integer fila) {

        if (isNuevo()) {

            MovimientoAlmacenDetalleTempDAO mADTDao = new MovimientoAlmacenDetalleTempDAOImp();

            int i = 0;
            for (AvmovMovNotaDespachoDet item : listaNotaDespachoDetalle) {
                if (i == fila) {

                    if (ndd.getRucCompanyia().equals(item.getRucCompanyia())
                            && ndd.getCodEstablecimiento().equals(item.getCodEstablecimiento())
                            && ndd.getCodArea() == item.getCodArea() && ndd.getCodCentroc() == item.getCodCentroc()
                            && ndd.getCodProducto().equals(item.getCodProducto()) && ndd.getCodAlmacen() == item.getCodAlmacen()) {

                        mADTDao.deleteMovimientoAlmacenDetalleTemp(item);
                        this.listaNotaDespachoDetalle.remove(i);
                        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Item eliminado"));
                        break;
                    }

                }
                i++;
            }
        } else {
            NotaDespachoDetalleDAO nDDDao = new NotaDespachoDetalleDAOImp();

            int i = 0;
            for (AvmovMovNotaDespachoDet item : listaNotaDespachoDetalle) {
                if (i == fila) {

                    if (ndd.getRucCompanyia().equals(item.getRucCompanyia())
                            && ndd.getCodEstablecimiento().equals(item.getCodEstablecimiento())
                            && ndd.getCodArea() == item.getCodArea() && ndd.getCodCentroc() == item.getCodCentroc()
                            && ndd.getCodProducto().equals(item.getCodProducto()) && ndd.getCodAlmacen() == item.getCodAlmacen()) {
                        nDDDao.deleteNotaDespachoDetalle(item);
                        this.listaNotaDespachoDetalle.remove(i);
                        FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Informacion", "Item eliminado"));
                        break;
                    }
                }
                i++;
            }
        }

    }

    public void verReporteNotaDespacho(AvmovMovNotaDespachoCab nd) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {

        reportesNotaDespacho rNotaDespacho = new reportesNotaDespacho();

        FacesContext facesContext = FacesContext.getCurrentInstance();
        ServletContext servletContext = (ServletContext) facesContext.getExternalContext().getContext();
        String ruta = servletContext.getRealPath("/reports/rpt_NotaDespacho.jasper");

        rNotaDespacho.getReporteNotaDespacho(ruta, nd.getNumVale());
        FacesContext.getCurrentInstance().responseComplete();

    }

}
